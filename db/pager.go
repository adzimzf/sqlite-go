package db

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/adzimzf/sqlite-go/constant"
	"os"
)

// ReadDatabaseHeader reads and parses the SQLite database header
func ReadDatabaseHeader(file *os.File) (*DatabaseHeader, error) {
	var header DatabaseHeader
	_, err := file.ReadAt(header.HeaderString[:], 0)
	if err != nil {
		return nil, err
	}

	// Verify the SQLite magic string
	if string(header.HeaderString[:])[:16] != constant.SQLiteMagic {
		return nil, fmt.Errorf("invalid SQLite database file")
	}

	// the header page is 2 bytes after the SQLiteMagicString
	if err := binary.Read(bytes.NewReader(header.HeaderString[16:18]), binary.BigEndian, &header.PageSize); err != nil {
		return nil, err
	}

	// Read other header fields
	//err = binary.Read(file, binary.BigEndian, &header.PageSize)
	//if err != nil {
	//	return nil, err
	//}

	// Continue reading the remaining header fields as needed
	// For simplicity, we skip parsing all fields in this example

	return &header, nil
}

// BTreePageType represents the type of a B-tree page
type BTreePageType byte

const (
	BTREE_INTERNAL_PAGE  BTreePageType = 2
	BTREE_LEAF_INDEX     BTreePageType = 10
	BTREE_INTERNAL_TABLE BTreePageType = 5
	BTREE_LEAF_TABLE     BTreePageType = 13
)

// BTreePage represents a generic B-tree page
type BTreePage struct {
	PageType byte
	// Other fields can be added as needed

}

type TableLeafPage struct {
	Header       TableHeader
	CellPointers []uint16
	Cells        []TableLeafCell
}

func NewTableLeafPage(file *os.File, pageSize, pageNumber int) (*TableLeafPage, error) {
	pageData, err := ReadBTreePage(file, pageSize, uint32(pageNumber))
	if err != nil {
		return nil, err
	}

	pageLeaf := &TableLeafPage{
		Header: TableHeader{
			PageType: BTREE_LEAF_TABLE,
		},
	}
	nCellBytes := pageData[3:5]
	filePageSize := 100
	if pageNumber == 1 {
		nCellBytes = pageData[filePageSize+3 : filePageSize+5]
	}

	pageLeaf.Header.CellCount = binary.BigEndian.Uint16(nCellBytes)
	pageLeaf.Header.PageNumber = uint16(pageNumber)
	pageLeaf.CellPointers = make([]uint16, pageLeaf.Header.CellCount)

	// Byte 4-7: Start of cell content area
	//cellContentStart := binary.BigEndian.Uint32(pageData[4:8])
	ptrOffsite := uint16(0)
	if pageNumber == 1 {
		//ptrOffsite = 100
	}
	// Cell pointers start at byte 8
	for i := 0; i < int(pageLeaf.Header.CellCount); i++ {
		offset := 8 + i*2
		if pageNumber == 1 {
			offset += 100
		}

		bytePtr := pageData[offset : offset+2]
		ptr := binary.BigEndian.Uint16(bytePtr)
		pageLeaf.CellPointers[i] = ptr - ptrOffsite
	}

	for _, ptr := range pageLeaf.CellPointers {
		if int(ptr) >= len(pageData) {
			continue // Invalid pointer
		}
		// read fist bytes to identify the payload size

		cellData := pageData[ptr:]

		n, size, err := ReadVarintAt(cellData, 0)
		if err != nil {
			return nil, err
		}
		cellData = cellData[n:]
		n, rowID, err := ReadVarintAt(cellData, 0)
		if err != nil {
			return nil, err
		}
		cellData = cellData[n : size+1]
		//log.Println(strconv.FormatInt(rowID, 10))

		pageLeaf.Cells = append(pageLeaf.Cells, TableLeafCell{
			Size:    size,
			RowID:   rowID,
			Payload: cellData,
		})
	}

	return pageLeaf, nil
}

func (t *TableLeafPage) GetRecords() ([]Record, error) {
	records := make([]Record, len(t.Cells))
	for i, cell := range t.Cells {
		recordHeader, err := NewRecordHeader(cell.Payload)
		if err != nil {
			return nil, err
		}
		records[i] = Record{
			RowID:   cell.RowID,
			Header:  recordHeader,
			Payload: cell.Payload,
		}
	}
	return records, nil
}

// GetRecordsFields return the records only for certain fields
// this method isn't performance wise, that being said it's still okay in early stage.
func (t *TableLeafPage) GetRecordsFields(fields []int64) ([]Record, error) {
	records, err := t.GetRecords()
	if err != nil {
		return nil, err
	}

	// an empty filter will return all
	if len(fields) == 0 {
		return records, nil
	}

	newRecords := make([]Record, len(records))
	for ri, record := range records {
		var newFields []RecordField
		for i := 0; i < len(fields); i++ {
			newFields = append(newFields, record.Header.Fields[fields[i]])
		}
		record.Header.Fields = newFields
		newRecords[ri] = record
	}
	return newRecords, nil
}

type TableHeader struct {
	PageType   BTreePageType
	CellCount  uint16
	PageNumber uint16
}

type TableLeafCell struct {
	Size    int64
	RowID   int64
	Payload []byte
}

// ReadBTreePage reads a specific page from the database file
func ReadBTreePage(file *os.File, pageSize int, pageNumber uint32) ([]byte, error) {
	offset := int64((int(pageNumber) - 1) * pageSize)
	if pageNumber == 1 {
		offset = 0
	}
	buf := make([]byte, pageSize)
	_, err := file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

package main

import (
	"encoding/binary"
	"os"
)

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
	pageLeaf.Header.CellCount = binary.BigEndian.Uint16(nCellBytes)
	pageLeaf.Header.PageNumber = uint16(pageNumber)
	pageLeaf.CellPointers = make([]uint16, pageLeaf.Header.CellCount)

	// Byte 4-7: Start of cell content area
	//cellContentStart := binary.BigEndian.Uint32(pageData[4:8])
	ptrOffsite := uint16(0)
	if pageNumber == 1 {
		ptrOffsite = 100
	}
	// Cell pointers start at byte 8
	for i := 0; i < int(pageLeaf.Header.CellCount); i++ {
		offset := 8 + i*2
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
		cellData = cellData[n:]
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

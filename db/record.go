package db

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
)

// Record represents a single record in sqlite_master
type Record struct {
	RowID   int64
	Header  RecordHeader
	Payload []byte
}

func (r *Record) FieldData(idx int) (any, FieldType, error) {
	field := r.Header.Fields[idx]
	switch field.FieldType {
	case Null:
		return nil, field.FieldType, nil
	case Int8:
		return int8(r.Payload[field.Offset : field.Offset+1][0]), field.FieldType, nil
	case Int16:
		return binary.BigEndian.Uint16(r.Payload[field.Offset : field.Offset+2]), field.FieldType, nil
	case Int24:
		return int32(binary.BigEndian.Uint16(r.Payload[field.Offset : field.Offset+3])), field.FieldType, nil
	case Int32:
		return int32(binary.BigEndian.Uint16(r.Payload[field.Offset : field.Offset+4])), field.FieldType, nil
	case Int48:
		return int64(binary.BigEndian.Uint16(r.Payload[field.Offset : field.Offset+6])), field.FieldType, nil
	case Int64:
		return int64(binary.BigEndian.Uint16(r.Payload[field.Offset : field.Offset+8])), field.FieldType, nil
	case Float64:
		return math.Float64frombits(uint64(binary.BigEndian.Uint16(r.Payload[field.Offset : field.Offset+8]))),
			field.FieldType, nil
	case String:
		data := r.Payload[field.Offset:]
		if idx+1 != len(r.Header.Fields) {
			data = r.Payload[field.Offset : uint64(field.Offset)+field.Size]
		}
		return string(data), field.FieldType, nil
	case Blob:
		data := r.Payload[field.Offset:]
		if idx+1 != len(r.Header.Fields) {
			data = r.Payload[field.Offset : uint64(field.Offset)+field.Size]
		}
		return data, field.FieldType, nil
	default:
		return nil, field.FieldType, fmt.Errorf("unimplemented")
	}
}

type RecordHeader struct {
	Fields []RecordField
}

func NewRecordHeader(payload []byte) (RecordHeader, error) {
	headerLengthUint64, varintSize := binary.Uvarint(payload)

	headerLength := headerLengthUint64
	data := payload[varintSize:headerLength]
	curOffset := headerLength

	var fields []RecordField

	for len(data) != 0 {
		serialType, at := binary.Uvarint(data)

		data = data[at:]

		var fieldSize uint64

		fieldType := FieldType(serialType)
		switch fieldType {
		case Null:
			fieldSize = 0
		case Int8:
			fieldSize = 1
		case Int16:
			fieldSize = 2
		case Int32:
			fieldSize = 3
		case Int48:
			fieldSize = 4
		case Int64:
			fieldSize = 8
		case Float64:
			fieldSize = 8
		case Zero:
			fieldSize = 0
		case One:
			fieldSize = 0
		default:
			if serialType >= 12 && serialType%2 == 0 {
				fieldSize = (serialType - 12) / 2
				fieldType = Blob
			} else if serialType >= 13 && serialType%2 == 1 {
				fieldSize = (serialType - 13) / 2
				fieldType = String
			} else {
				return RecordHeader{}, fmt.Errorf("serial type %d isn't supported", serialType)
			}
		}

		fields = append(fields, RecordField{
			Offset:    uint16(curOffset),
			FieldType: fieldType,
			Size:      fieldSize,
		})

		curOffset += fieldSize

	}
	return RecordHeader{
		Fields: fields,
	}, nil

}

type RecordField struct {
	Offset    uint16
	Size      uint64
	FieldType FieldType
}

type FieldType int32

const (
	Null FieldType = iota
	Int8
	Int16
	Int24
	Int32
	Int48
	Int64
	Float64
	Zero
	One
	_
	_
	Blob
	String
)

// ParseRecords parses records from a table leaf page
func ParseRecords(file *os.File, pageData []byte, pageNumber int) ([]Record, error) {
	records := []Record{}

	if len(pageData) < 1 {
		return records, fmt.Errorf("page data too short")
	}
	fmt.Printf("page data: % X\n", pageData)
	// Byte 0: Page Type
	pageType := BTreePageType(pageData[0])
	if pageType != BTREE_LEAF_TABLE {
		return records, fmt.Errorf("not a table leaf page")
	}

	// Byte 1: First freeblock
	// Byte 2-3: Number of cells
	if len(pageData) < 4 {
		return records, fmt.Errorf("page data too short for header")
	}
	nCellBytes := pageData[3:5]
	numCells := binary.BigEndian.Uint16(nCellBytes)

	// Byte 4-7: Start of cell content area
	//cellContentStart := binary.BigEndian.Uint32(pageData[4:8])

	ptrOffsite := 0
	if pageNumber == 1 {
		ptrOffsite = 100
	}

	// Cell pointers start at byte 8
	cellPointers := make([]uint16, numCells)
	for i := 0; i < int(numCells); i++ {
		offset := 8 + i*2
		bytePtr := pageData[offset : offset+2]
		ptr := binary.BigEndian.Uint16(bytePtr)
		cellPointers[i] = ptr - uint16(ptrOffsite)
	}

	// Iterate over each cell
	for _, ptr := range cellPointers {
		if int(ptr) >= len(pageData) {
			continue // Invalid pointer
		}
		// read fist bytes to identify the payload size

		cellData := pageData[ptr:]

		n, _, err := ReadVarintAt(cellData, 0)
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

		recordHeader, err := NewRecordHeader(cellData)

		rec := Record{
			RowID:   rowID,
			Header:  recordHeader,
			Payload: cellData,
		}

		records = append(records, rec)
	}

	return records, nil
}

// ReadVarintAt reads a variable-length integer from the buffer starting at the given offset.
// It returns the number of bytes read, the decoded integer value, and an error if any.
func ReadVarintAt(buffer []byte, offset int) (bytesRead uint8, value int64, err error) {
	var size uint8 = 0
	var result int64 = 0

	// Maximum 8 bytes to prevent overflow (7 bits per byte * 8 = 56 bits)
	for size < 8 {
		if offset >= len(buffer) {
			return size, result, errors.New("buffer too small to contain varint")
		}

		byteVal := buffer[offset]
		dataBits := byteVal & 0x7F              // Extract lower 7 bits
		result |= int64(dataBits) << (7 * size) // Accumulate into result

		size++
		offset++

		if byteVal&0x80 == 0 { // MSB is 0, last byte
			return size, result, nil
		}
	}

	// If we reach here, varint is too long
	return size, result, errors.New("varint is too long (exceeds 8 bytes)")
}

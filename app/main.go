package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	// Available if you need it!
	// "github.com/xwb1989/sqlparser"
)

// SQLite header constants
const (
	HeaderSize         = 100
	SQLiteMagic        = "SQLite format 3\x00"
	SqliteMasterName   = "sqlite_master"
	SqliteInternalName = "sqlite_sequence"
	DefaultPageSize    = 4096
)

// DatabaseHeader represents the SQLite database header structure
type DatabaseHeader struct {
	HeaderString        [100]byte
	PageSize            uint16
	FileFormatWrite     byte
	FileFormatRead      byte
	Reserved1           byte
	MaxEmbeddedPayload  byte
	MinEmbeddedPayload  byte
	LeafPayloadFraction byte
	FileChangeCounter   uint32
	SizeOfPageCache     uint32
	SchemaCookie        uint32
	SchemaFormat        uint32
	DefaultPageCache    byte
	IncrementalVacuum   byte
	ApplicationID       uint32
	VersionValidFor     uint32
	VersionUsed         uint32
}

// Usage: your_program.sh sample.db .dbinfo
func main() {
	databaseFilePath := os.Args[1]
	command := os.Args[2]

	switch command {
	case ".dbinfo":
		databaseFile, err := os.Open(databaseFilePath)
		if err != nil {
			log.Fatal(err)
		}

		databaseHeader, err := ReadDatabaseHeader(databaseFile)
		if err != nil {
			log.Fatal(err)
		}

		rootPage, err := NewTableLeafPage(databaseFile, int(databaseHeader.PageSize), 1)
		if err != nil {
			log.Fatal(err)
		}

		//rootPage.

		//log.Println(rootPage)

		//countTables, err := CountTables(databaseFile, databaseHeader)
		//if err != nil {
		//	log.Fatal(err)
		//}

		records, err := rootPage.GetRecords()
		if err != nil {
			log.Fatal(err)
		}

		// You can use print statements as follows for debugging, they'll be visible when running tests.
		fmt.Println("Logs from your program will appear here!")

		// Uncomment this to pass the first stage
		fmt.Printf("database page size: %v\n", databaseHeader.PageSize)

		fmt.Printf("number of tables: %v\n", len(records))
	default:
		fmt.Println("Unknown command", command)
		os.Exit(1)
	}
}

// ReadDatabaseHeader reads and parses the SQLite database header
func ReadDatabaseHeader(file *os.File) (*DatabaseHeader, error) {
	var header DatabaseHeader
	_, err := file.ReadAt(header.HeaderString[:], 0)
	if err != nil {
		return nil, err
	}

	// Verify the SQLite magic string
	if string(header.HeaderString[:])[:16] != SQLiteMagic {
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

// GetRootPageNumber reads the root page number from the header (bytes 28-31)
func GetRootPageNumber(file *os.File) (uint32, error) {
	buf := make([]byte, 4)
	_, err := file.ReadAt(buf, 28)
	if err != nil {
		return 0, err
	}
	rootPage := binary.BigEndian.Uint32(buf)
	return rootPage, nil
}

// ReadBTreePage reads a specific page from the database file
func ReadBTreePage(file *os.File, pageSize int, pageNumber uint32) ([]byte, error) {
	offset := int64((int(pageNumber)-1)*pageSize) + 100 // header file
	buf := make([]byte, pageSize)
	_, err := file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// ParseRecordPayload parses the payload of a record to extract fields
//func ParseRecordPayload(payload []byte) (Record, error) {
//	record := Record{}
//
//	if len(payload) < 1 {
//		return record, fmt.Errorf("payload too short")
//	}
//
//	// Number of columns is determined by the record header
//	// Record format: [serial_type list][record data]
//	// For sqlite_master, there are 5 columns: type, name, tbl_name, rootpage, sql
//
//	// First, read the serial types
//	// Read varint serial types
//	// For simplicity, assume all serial types are single bytes
//
//	// Determine the number of columns by counting serial types until the data starts
//	// For sqlite_master, it's fixed at 5 columns
//
//	// Serial types for sqlite_master:
//	// 1: type (TEXT)
//	// 2: name (TEXT)
//	// 3: tbl_name (TEXT)
//	// 4: rootpage (INTEGER)
//	// 5: sql (TEXT)
//
//	// Extract serial types
//	serialTypes := []uint8{}
//	offset := 0
//	for i := 0; i < 5 && offset < len(payload); i++ {
//		serialType := payload[offset]
//		serialTypes = append(serialTypes, serialType)
//		offset++
//	}
//
//	// Now, extract the actual data based on serial types
//	// Simplified parsing: assume TEXT serial types are variable length with 1-byte length prefix
//	// and INTEGER is 4 bytes (big endian)
//	for i, st := range serialTypes {
//		switch st {
//		case 2: // TEXT
//			if offset >= len(payload) {
//				return record, fmt.Errorf("unexpected end of payload for TEXT field")
//			}
//			length := int(payload[offset])
//			offset++
//			if offset+length > len(payload) {
//				return record, fmt.Errorf("unexpected end of payload for TEXT data")
//			}
//			text := string(payload[offset : offset+length])
//			offset += length
//
//			switch i {
//			case 0:
//				record.Type = text
//			case 1:
//				record.Name = text
//			case 2:
//				record.TblName = text
//			case 4:
//				record.SQL = text
//			}
//
//		case 1: // NULL
//			// Skip
//		case 4: // INTEGER (4 bytes)
//			if offset+4 > len(payload) {
//				return record, fmt.Errorf("unexpected end of payload for INTEGER field")
//			}
//			val := binary.BigEndian.Uint32(payload[offset : offset+4])
//			offset += 4
//			if i == 3 { // rootpage
//				record.RootPage = val
//			}
//
//		default:
//			// Unsupported serial type
//			return record, fmt.Errorf("unsupported serial type: %d", st)
//		}
//	}
//
//	return record, nil
//}

// CountTables counts the number of user-defined tables in the SQLite database file
func CountTables(file *os.File, header *DatabaseHeader) (int, error) {

	pageSize := int(header.PageSize)
	if pageSize <= 0 {
		pageSize = DefaultPageSize
	}

	// Step 2: Get the root page number for sqlite_master
	//rootPage, err := GetRootPageNumber(file)
	//if err != nil {
	//	return 0, err
	//}
	//allFile, _ := ioutil.ReadAll(file)
	//log.Printf("% X\n", allFile)

	rootPage := uint32(1)

	// Step 3: Read the root page
	pageData, err := ReadBTreePage(file, pageSize, rootPage)
	if err != nil {
		return 0, err
	}

	// Step 4: Parse the root page to get child pages if it's an internal page
	pageType := BTreePageType(pageData[0])
	var leafPages []uint32

	if pageType == BTREE_INTERNAL_TABLE {
		// Internal page: iterate through cell pointers to find child pages
		numCells := binary.BigEndian.Uint16(pageData[3:5])
		for i := 0; i < int(numCells); i++ {
			// Cell pointer starts at byte 8 + i*2
			ptr := binary.BigEndian.Uint16(pageData[8+i*2 : 10+i*2])
			if int(ptr) >= len(pageData) {
				continue
			}

			// Each cell on an internal page starts with a child page number (4 bytes)
			childPage := binary.BigEndian.Uint32(pageData[ptr : ptr+4])
			leafPages = append(leafPages, childPage)
		}
	} else if pageType == BTREE_LEAF_TABLE {
		// Leaf page: add the root page itself
		leafPages = append(leafPages, rootPage)
	} else {
		return 0, fmt.Errorf("unexpected page type: %d", pageType)
	}

	// Step 5: Iterate through all leaf pages and parse records
	tableCount := 0

	for _, lp := range leafPages {
		lpData, err := ReadBTreePage(file, pageSize, lp)
		if err != nil {
			log.Printf("Error reading leaf page %d: %v", lp, err)
			continue
		}

		records, err := ParseRecords(file, lpData, 1)
		if err != nil {
			log.Printf("Error parsing records on page %d: %v", lp, err)
			continue
		}

		tableCount = len(records)

		//for _, record := range records {
		//	if strings.ToLower(record.Type) == "table" && !strings.HasPrefix(record.Name, "sqlite_") {
		//		tableCount++
		//	}
		//}
	}

	return tableCount, nil
}

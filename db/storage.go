package db

import (
	"fmt"
	"os"
)

type DB struct {
	header *DatabaseHeader
	file   *os.File
}

func NewDB(file *os.File) (*DB, error) {
	databaseHeader, err := ReadDatabaseHeader(file)
	if err != nil {
		return nil, err
	}

	return &DB{header: databaseHeader, file: file}, nil
}

func (d *DB) FindSQLiteMaster() (*TableLeafPage, error) {
	rootPage, err := NewTableLeafPage(d.file, int(d.header.PageSize), 1)
	if err != nil {
		return nil, err
	}
	return rootPage, nil
}

func (d *DB) FindPageID(pageID int) (*TableLeafPage, error) {
	rootPage, err := NewTableLeafPage(d.file, int(d.header.PageSize), pageID)
	if err != nil {
		return nil, err
	}
	return rootPage, nil
}

func (d *DB) FindTablePage(name string) (*TableLeafPage, error) {

	sqLiteMaster, err := d.FindSQLiteMaster()
	if err != nil {
		return nil, err
	}

	records, err := sqLiteMaster.GetRecords()
	if err != nil {
		return nil, err
	}

	for _, record := range records {
		data, fieldType, err := record.FieldData(2)
		if err != nil {
			return nil, err
		}

		if fieldType != String {
			return nil, fmt.Errorf("invalid field type: %v", fieldType)
		}

		if data.(string) == name {
			pageNumber, _, err := record.FieldData(3)
			if err != nil {
				return nil, err
			}

			leafPage, err := d.FindPageID(int(pageNumber.(int8)))
			if err != nil {
				return nil, err
			}
			return leafPage, nil

		}
	}

	return nil, fmt.Errorf("table not found: %v", name)
}

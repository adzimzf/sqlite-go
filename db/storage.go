package db

import (
	"fmt"
	"github.com/adzimzf/sqlite-go/sql"
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

func (d *DB) FindTableSchema(tableName string) (TableSchemaInfo, error) {

	if tableName == "sqlite_master" {
		return d.FindSQLiteSchema()
	}

	sqliteMaster, err := d.FindSQLiteMaster()
	if err != nil {
		return TableSchemaInfo{}, err
	}

	records, err := sqliteMaster.GetRecords()
	if err != nil {
		return TableSchemaInfo{}, err
	}
	for _, record := range records {
		data, fType, err2 := record.FieldData(1)
		if err2 != nil {
			return TableSchemaInfo{}, err2
		}
		if fType != String {
			return TableSchemaInfo{}, fmt.Errorf("invalid field type: %v", fType)
		}

		if data.(string) == tableName {
			rawQuery, _, err := record.FieldData(4)
			if err != nil {
				return TableSchemaInfo{}, err
			}
			rootPage, _, err := record.FieldData(3)
			if err != nil {
				return TableSchemaInfo{}, err
			}

			tableSchema := TableSchemaInfo{
				PageID: int64(rootPage.(int8)),
				RawSQL: rawQuery.(string),
			}

			parse, err := sql.Parse(rawQuery.(string))
			if err != nil {
				return TableSchemaInfo{}, err
			}

			err = TableSchemaVisitor(parse, &tableSchema)
			if err != nil {
				return TableSchemaInfo{}, err
			}

			return tableSchema, nil
		}
	}
	return TableSchemaInfo{}, fmt.Errorf("table not found: %v", tableName)
}

func (d *DB) FindSQLiteSchema() (TableSchemaInfo, error) {
	tableSchema := TableSchemaInfo{
		PageID: int64(1),
		RawSQL: `CREATE TABLE sqlite_schema (type text, name text, tbl_name text, rootpage integer, sql text);`,
	}

	parse, err := sql.Parse(tableSchema.RawSQL)
	if err != nil {
		return TableSchemaInfo{}, err
	}

	err = TableSchemaVisitor(parse, &tableSchema)
	if err != nil {
		return TableSchemaInfo{}, err
	}

	return tableSchema, nil
}

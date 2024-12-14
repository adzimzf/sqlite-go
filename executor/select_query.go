package executor

import (
	"github.com/adzimzf/sqlite-go/db"
	"os"
)

func ExecuteSelectQuery(file *os.File, info db.QueryInfo) (db.Rows, error) {
	newDB, err := db.NewDB(file)
	if err != nil {
		return nil, err
	}

	var records []db.Record
	var rows db.Rows
	tableInfoMap := map[string]db.TableSchemaInfo{}

	// if the select into sqlite_master no need to go to the master
	for _, table := range info.JoinTables {

		if table == "sqlite_master" {
			sqLiteMaster, err := newDB.FindSQLiteMaster()
			if err != nil {
				return nil, err
			}
			record, err := sqLiteMaster.GetRecords()
			if err != nil {
				return nil, err
			}

			sqLiteSchema, err := newDB.FindSQLiteSchema()
			if err != nil {
				return nil, err
			}

			rows1, err := db.RecordsToRows(record, sqLiteSchema)
			if err != nil {
				return nil, err
			}
			rows = append(rows, rows1...)
			continue
		}

		tableInfo, err := newDB.FindTableSchema(table)
		if err != nil {
			return nil, err
		}
		tableInfoMap[table] = tableInfo

		page, err := newDB.FindTablePage(table)
		if err != nil {
			return nil, err
		}
		fieldNameByTable := info.FieldNameByTable(table)
		indexList := tableInfo.ColumnIndex(fieldNameByTable...)

		record, err := page.GetRecordsFields(indexList)
		if err != nil {
			return nil, err
		}
		rows1, err := db.RecordsToRows(record, tableInfo)
		if err != nil {
			return nil, err
		}
		rows = append(rows, rows1...)

		if len(info.SelectFields) == 1 && info.SelectFields[0].ColName == "*" {
			newSelectedFields := []*db.SelectFieldExpression{}
			for _, column := range tableInfo.Columns {
				newSelectedFields = append(newSelectedFields, &db.SelectFieldExpression{
					ColName: column.Name,
				})
			}
			info.SelectFields = newSelectedFields
		}
	}

	for _, field := range info.SelectFields {
		if field.IsAgg {
			if field.ColName == "*" && field.AggType == db.COUNT_AGGREGATE {
				return db.Rows{
					{
						db.NewInt64Tuple(int64(len(records))),
					},
				}, nil
			}
		}
	}

	return rows, nil
}

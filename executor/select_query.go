package executor

import (
	"github.com/adzimzf/sqlite-go/db"
	"os"
)

func ExecuteSelectQuery(file *os.File, info QueryInfo) (Rows, error) {
	newDB, err := db.NewDB(file)
	if err != nil {
		return nil, err
	}

	var records []db.Record
	var rows Rows

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
			records = append(records, record...)
			continue
		}
		page, err := newDB.FindTablePage(table)
		if err != nil {
			return nil, err
		}
		record, err := page.GetRecords()
		if err != nil {
			return nil, err
		}
		records = append(records, record...)
	}

	rows, err = RecordsToRows(records)
	if err != nil {
		return nil, err
	}

	for _, field := range info.SelectFields {
		if field.IsAgg {
			if field.ColName == "*" && field.AggType == COUNT_AGGREGATE {
				return Rows{
					{
						NewInt64Tuple(int64(len(records))),
					},
				}, nil
			}
		}
	}

	return rows, nil
}

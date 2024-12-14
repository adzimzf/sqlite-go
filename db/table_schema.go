package db

type DDLAction int

const (
	DDLCreateTable DDLAction = iota
)

type DDLInfo struct {
	Action DDLAction
	Table  TableSchemaInfo
}

type TableSchemaInfo struct {
	PageID              int64
	RawSQL              string
	IsAutoIncrement     bool
	AutoIncrementColumn uint64
	Name                string
	PrimaryKey          TableColumnInfo
	Columns             []TableColumnInfo
}

func (info TableSchemaInfo) ColumnIndex(fields ...string) []int64 {
	var indexes []int64
	for _, col := range info.Columns {
		for _, field := range fields {
			if col.Name == field {
				indexes = append(indexes, col.Idx)
			}
		}
	}
	return indexes
}

func (info TableSchemaInfo) HasPrimaryKey() bool {
	return info.PrimaryKey != (TableColumnInfo{})
}

type TableColumnInfo struct {
	Idx  int64
	Name string
	Type FieldType
}

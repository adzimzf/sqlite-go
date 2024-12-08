package executor

import "github.com/adzimzf/sqlite-go/db"

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
	Columns             []TableColumnInfo
}

type TableColumnInfo struct {
	Idx  uint64
	Name string
	Type db.FieldType
}

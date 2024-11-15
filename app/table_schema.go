package main

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
	Columns             []TableColumnInfo
}

type TableColumnInfo struct {
	Idx  uint64
	Name string
	Type FieldType
}

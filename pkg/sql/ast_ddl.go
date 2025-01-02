package sql

// DDL represents a CREATE, ALTER, DROP, RENAME or TRUNCATE statement.
// Table is set for AlterStr, DropStr, RenameStr, TruncateStr
// NewName is set for AlterStr, CreateStr, RenameStr.
// VindexSpec is set for CreateVindexStr, DropVindexStr, AddColVindexStr, DropColVindexStr
// VindexCols is set for AddColVindexStr
type DDL struct {
	Action    string
	Table     TableName
	NewName   TableName
	IfExists  bool
	TableSpec *TableSpec
	//PartitionSpec *PartitionSpec
	//VindexSpec    *VindexSpec
	VindexCols []ColIdent
}

func (node *DDL) iStatement() {

}

// DDL strings.
const (
	CreateStr        = "create"
	AlterStr         = "alter"
	DropStr          = "drop"
	RenameStr        = "rename"
	TruncateStr      = "truncate"
	CreateVindexStr  = "create vindex"
	AddColVindexStr  = "add vindex"
	DropColVindexStr = "drop vindex"

	// Vindex DDL param to specify the owner of a vindex
	VindexOwnerStr = "owner"
)

func (node *DDL) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Table,
		node.NewName,
	)
}

// TableSpec describes the structure of a table from a CREATE TABLE statement
type TableSpec struct {
	Columns []*ColumnDefinition
	//Indexes []*IndexDefinition
	Options string
}

// AddColumn appends the given column to the list in the spec
func (ts *TableSpec) AddColumn(cd *ColumnDefinition) {
	ts.Columns = append(ts.Columns, cd)
}

// AddIndex appends the given index to the list in the spec
//func (ts *TableSpec) AddIndex(id *IndexDefinition) {
//	ts.Indexes = append(ts.Indexes, id)
//}

func (ts *TableSpec) walkSubtree(visit Visit) error {
	if ts == nil {
		return nil
	}

	for _, n := range ts.Columns {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}

	//for _, n := range ts.Indexes {
	//	if err := Walk(visit, n); err != nil {
	//		return err
	//	}
	//}

	return nil
}

// ColumnDefinition describes a column in a CREATE TABLE statement
type ColumnDefinition struct {
	Name ColIdent
	Type ColumnType
}

func (col *ColumnDefinition) walkSubtree(visit Visit) error {
	if col == nil {
		return nil
	}
	return Walk(
		visit,
		col.Name,
		&col.Type,
	)
}

// ColumnType represents a sql type in a CREATE TABLE statement
// All optional fields are nil if not specified
type ColumnType struct {
	// The base type string
	Type string

	// Generic field options.
	NotNull       BoolVal
	Autoincrement BoolVal
	Default       *SQLVal
	OnUpdate      *SQLVal
	//Comment       *SQLVal

	// Numeric field options
	Length   *SQLVal
	Unsigned BoolVal
	Zerofill BoolVal
	Scale    *SQLVal

	// Text field options
	Charset string
	Collate string

	// Enum values
	EnumValues []string

	// Key specification
	KeyOpt ColumnKeyOption
}

func NewIntegerColumn() ColumnType {
	return ColumnType{
		Type: "INTEGER",
	}
}

func NewFloatColumn() ColumnType {
	return ColumnType{
		Type: "FLOAT",
	}
}

func NewTextColumn() ColumnType {
	return ColumnType{
		Type: "TEXT",
	}
}

func NewBlobColumn() ColumnType {
	return ColumnType{
		Type: "BLOB",
	}
}

// DescribeType returns the abbreviated type information as required for
// describe table
func (ct *ColumnType) DescribeType() string {
	//buf := NewTrackedBuffer(nil)
	//buf.Myprintf("%s", ct.Type)
	//if ct.Length != nil && ct.Scale != nil {
	//	buf.Myprintf("(%v,%v)", ct.Length, ct.Scale)
	//} else if ct.Length != nil {
	//	buf.Myprintf("(%v)", ct.Length)
	//}
	//
	//opts := make([]string, 0, 16)
	//if ct.Unsigned {
	//	opts = append(opts, keywordStrings[UNSIGNED])
	//}
	//if ct.Zerofill {
	//	opts = append(opts, keywordStrings[ZEROFILL])
	//}
	//if len(opts) != 0 {
	//	buf.Myprintf(" %s", strings.Join(opts, " "))
	//}
	//return buf.String()
	panic("unimplemented")
}

// SQLType returns the sqltypes type code for the given column
//func (ct *ColumnType) SQLType() querypb.Type {
//	switch ct.Type {
//	case keywordStrings[TINYINT]:
//		if ct.Unsigned {
//			return sqltypes.Uint8
//		}
//		return sqltypes.Int8
//	case keywordStrings[SMALLINT]:
//		if ct.Unsigned {
//			return sqltypes.Uint16
//		}
//		return sqltypes.Int16
//	case keywordStrings[MEDIUMINT]:
//		if ct.Unsigned {
//			return sqltypes.Uint24
//		}
//		return sqltypes.Int24
//	case keywordStrings[INT]:
//		fallthrough
//	case keywordStrings[INTEGER]:
//		if ct.Unsigned {
//			return sqltypes.Uint32
//		}
//		return sqltypes.Int32
//	case keywordStrings[BIGINT]:
//		if ct.Unsigned {
//			return sqltypes.Uint64
//		}
//		return sqltypes.Int64
//	case keywordStrings[TEXT]:
//		return sqltypes.Text
//	case keywordStrings[TINYTEXT]:
//		return sqltypes.Text
//	case keywordStrings[MEDIUMTEXT]:
//		return sqltypes.Text
//	case keywordStrings[LONGTEXT]:
//		return sqltypes.Text
//	case keywordStrings[BLOB]:
//		return sqltypes.Blob
//	case keywordStrings[TINYBLOB]:
//		return sqltypes.Blob
//	case keywordStrings[MEDIUMBLOB]:
//		return sqltypes.Blob
//	case keywordStrings[LONGBLOB]:
//		return sqltypes.Blob
//	case keywordStrings[CHAR]:
//		return sqltypes.Char
//	case keywordStrings[VARCHAR]:
//		return sqltypes.VarChar
//	case keywordStrings[BINARY]:
//		return sqltypes.Binary
//	case keywordStrings[VARBINARY]:
//		return sqltypes.VarBinary
//	case keywordStrings[DATE]:
//		return sqltypes.Date
//	case keywordStrings[TIME]:
//		return sqltypes.Time
//	case keywordStrings[DATETIME]:
//		return sqltypes.Datetime
//	case keywordStrings[TIMESTAMP]:
//		return sqltypes.Timestamp
//	case keywordStrings[YEAR]:
//		return sqltypes.Year
//	case keywordStrings[FLOAT_TYPE]:
//		return sqltypes.Float32
//	case keywordStrings[DOUBLE]:
//		return sqltypes.Float64
//	case keywordStrings[DECIMAL]:
//		return sqltypes.Decimal
//	case keywordStrings[BIT]:
//		return sqltypes.Bit
//	case keywordStrings[ENUM]:
//		return sqltypes.Enum
//	case keywordStrings[SET]:
//		return sqltypes.Set
//	case keywordStrings[JSON]:
//		return sqltypes.TypeJSON
//	case keywordStrings[GEOMETRY]:
//		return sqltypes.Geometry
//	case keywordStrings[POINT]:
//		return sqltypes.Geometry
//	case keywordStrings[LINESTRING]:
//		return sqltypes.Geometry
//	case keywordStrings[POLYGON]:
//		return sqltypes.Geometry
//	case keywordStrings[GEOMETRYCOLLECTION]:
//		return sqltypes.Geometry
//	case keywordStrings[MULTIPOINT]:
//		return sqltypes.Geometry
//	case keywordStrings[MULTILINESTRING]:
//		return sqltypes.Geometry
//	case keywordStrings[MULTIPOLYGON]:
//		return sqltypes.Geometry
//	}
//	panic("unimplemented type " + ct.Type)
//}

func (ct *ColumnType) walkSubtree(visit Visit) error {
	return nil
}

// ColumnKeyOption indicates whether or not the given column is defined as an
// index element and contains the type of the option
type ColumnKeyOption int

const (
	ColKeyNone ColumnKeyOption = iota
	ColKeyPrimary
	colKeySpatialKey
	colKeyUnique
	colKeyUniqueKey
	colKey
)

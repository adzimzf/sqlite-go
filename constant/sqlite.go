package constant

// SQLite header constants
const (
	HeaderSize         = 100
	SQLiteMagic        = "SQLite format 3\x00"
	SqliteMasterName   = "sqlite_master"
	SqliteInternalName = "sqlite_sequence"
	DefaultPageSize    = 4096
)

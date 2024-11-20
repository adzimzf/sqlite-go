package sql

var tokens = map[string]int{
	"SELECT":        SELECT,
	"FROM":          FROM,
	"*":             STAR,
	",":             COMMA,
	"AS":            AS,
	"COUNT":         COUNT,
	"(":             LPAREN,
	")":             RPAREN,
	"CREATE":        CREATE,
	"PRIMARY":       PRIMARY,
	"KEY":           KEY,
	"INTEGER":       INTEGER,
	"BLOB":          BLOB,
	"TEXT":          TEXT,
	"AUTOINCREMENT": AUTOINCREMENT,
	"TABLE":         TABLE,
}

package sql

// Where represents a WHERE or HAVING clause.
type Where struct {
	Type string
	Expr Expr
}

// Where.Type
const (
	WhereStr  = "where"
	HavingStr = "having"
)

// NewWhere creates a WHERE or HAVING clause out
// of a Expr. If the expression is nil, it returns nil.
func NewWhere(typ string, expr Expr) *Where {
	if expr == nil {
		return nil
	}
	return &Where{Type: typ, Expr: expr}
}

func (node *Where) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
	)
}

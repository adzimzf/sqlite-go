package sql

// AndExpr represents an AND expression.
type AndExpr struct {
	Left, Right Expr
}

func (node *AndExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Left,
		node.Right,
	)
}

func (node *AndExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Left, &node.Right)
}

func (node *AndExpr) iExpr() {}

// OrExpr represents an OR expression.
type OrExpr struct {
	Left, Right Expr
}

func (node *OrExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Left,
		node.Right,
	)
}

func (node *OrExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Left, &node.Right)
}

func (node *OrExpr) iExpr() {}

// NotExpr represents a NOT expression.
type NotExpr struct {
	Expr Expr
}

func (node *NotExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
	)
}

func (node *NotExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Expr)
}
func (node *NotExpr) iExpr() {}

// ComparisonExpr represents a two-value comparison expression.
type ComparisonExpr struct {
	Operator    string
	Left, Right Expr
	Escape      Expr
}

// ComparisonExpr.Operator
const (
	EqualStr             = "="
	LessThanStr          = "<"
	GreaterThanStr       = ">"
	LessEqualStr         = "<="
	GreaterEqualStr      = ">="
	NotEqualStr          = "!="
	NullSafeEqualStr     = "<=>"
	InStr                = "in"
	NotInStr             = "not in"
	LikeStr              = "like"
	NotLikeStr           = "not like"
	RegexpStr            = "regexp"
	NotRegexpStr         = "not regexp"
	JSONExtractOp        = "->"
	JSONUnquoteExtractOp = "->>"
)

func (node *ComparisonExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Left,
		node.Right,
		node.Escape,
	)
}

func (node *ComparisonExpr) replace(from, to Expr) bool {
	return replaceExprs(from, to, &node.Left, &node.Right, &node.Escape)
}

func (node *ComparisonExpr) iExpr() {}

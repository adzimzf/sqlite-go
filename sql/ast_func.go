package sql

// FuncExpr represents a function call.
type FuncExpr struct {
	Qualifier TableIdent
	Name      ColIdent
	Distinct  bool
	Exprs     SelectExprs
}

func (node *FuncExpr) iExpr() {

}

func (node *FuncExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Qualifier,
		node.Name,
		node.Exprs,
	)
}

func (node *FuncExpr) replace(from, to Expr) bool {
	for _, sel := range node.Exprs {
		aliased, ok := sel.(*AliasedExpr)
		if !ok {
			continue
		}
		if replaceExprs(from, to, &aliased.Expr) {
			return true
		}
	}
	return false
}

// Aggregates is a map of all aggregate functions.
var Aggregates = map[string]bool{
	"avg":          true,
	"bit_and":      true,
	"bit_or":       true,
	"bit_xor":      true,
	"count":        true,
	"group_concat": true,
	"max":          true,
	"min":          true,
	"std":          true,
	"stddev_pop":   true,
	"stddev_samp":  true,
	"stddev":       true,
	"sum":          true,
	"var_pop":      true,
	"var_samp":     true,
	"variance":     true,
}

// IsAggregate returns true if the function is an aggregate.
func (node *FuncExpr) IsAggregate() bool {
	return Aggregates[node.Name.Lowered()]
}

// replaceExprs is a convenience function used by implementors
// of the replace method.
func replaceExprs(from, to Expr, exprs ...*Expr) bool {
	for _, expr := range exprs {
		if *expr == nil {
			continue
		}
		if *expr == from {
			*expr = to
			return true
		}
		if (*expr).replace(from, to) {
			return true
		}
	}
	return false
}

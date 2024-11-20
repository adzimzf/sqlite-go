package sql

// SelectStatement any SELECT statement.
type SelectStatement interface {
	//iSelectStatement()
	iStatement()
	//iInsertRows()
	//AddOrder(*Order)
	//SetLimit(*Limit)
	SQLNode
}

// Select represents a SELECT statement.
type Select struct {
	Cache string
	//Comments    Comments
	Distinct    string
	Hints       string
	SelectExprs SelectExprs
	From        TableExprs
	//Where       *Where
	//GroupBy     GroupBy
	//Having      *Where
	//OrderBy     OrderBy
	//Limit       *Limit
	Lock string
}

func (node *Select) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		//node.Comments,
		node.SelectExprs,
		node.From,
		//node.Where,
		//node.GroupBy,
		//node.Having,
		//node.OrderBy,
		//node.Limit,
	)
}
func (n *Select) iStatement() {}

// SelectExprs represents SELECT expressions.
type SelectExprs []SelectExpr

func NewSelectExprs(expr ...SelectExpr) SelectExprs {
	exps := make([]SelectExpr, len(expr))
	for i := 0; i < len(exps); i++ {
		exps[i] = expr[i]
	}
	return exps
}

func (node SelectExprs) walkSubtree(visit Visit) error {
	for _, n := range node {
		if err := Walk(visit, n); err != nil {
			return err
		}
	}
	return nil
}

// SelectExpr represents a SELECT expression.
type SelectExpr interface {
	iSelectExpr()
	SQLNode
}

// AliasedExpr defines an aliased SELECT expression.
type AliasedExpr struct {
	Expr Expr
	As   ColIdent
}

func (node *AliasedExpr) iSelectExpr() {}

func (node *AliasedExpr) walkSubtree(visit Visit) error {
	if node == nil {
		return nil
	}
	return Walk(
		visit,
		node.Expr,
		node.As,
	)
}

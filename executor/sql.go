package executor

import (
	"fmt"
	sqlparser "github.com/adzimzf/sqlite-go/sql"
)

type QueryType int32

const (
	SELECT QueryType = iota
	CREATE_TABLE
	INSERT
	DELETE
	UPDATE
)

type QueryInfo struct {
	QueryType    QueryType
	SelectFields []*SelectFieldExpression // SELECT
	//SetExpressions_      []*SetExpression         // UPDATE
	//NewTable_            *string                  // CREATE TABLE
	//ColDefExpressions_   []*ColDefExpression      // CREATE TABLE
	//IndexDefExpressions_ []*IndexDefExpression    // CREATE TABLE
	//TargetCols_          []*string                // INSERT
	//Values_              []*types.Value           // INSERT
	//OnExpressions_       *BinaryOpExpression      // SELECT (with JOIN)
	JoinTables []string // SELECT
	//WhereExpression_     *BinaryOpExpression      // SELECT, UPDATE, DELETE
	//LimitNum_            int32                    // SELECT
	//OffsetNum_           int32                    // SELECT
	//OrderByExpressions_  []*OrderByExpression     // SELECT
}

type SelectFieldExpression struct {
	IsAgg     bool
	AggType   AggregationType
	TableName string // if specified
	ColName   string
}

type AggregationType int32

/** The type of the log record. */
const (
	COUNT_AGGREGATE AggregationType = iota
	SUM_AGGREGATE
	MIN_AGGREGATE
	MAX_AGGREGATE
)

func (a AggregationType) String() string {
	aggNames := map[AggregationType]string{
		COUNT_AGGREGATE: "count",
	}

	s, ok := aggNames[a]
	if ok {
		return s
	}
	return "unknown"
}

func ExtractQueryInfo(sql string) (QueryInfo, error) {
	var res QueryInfo
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return res, err
	}
	err = RootSQLVisitor(stmt, &res)
	if err != nil {
		return QueryInfo{}, err
	}

	return res, nil
}

func RootSQLVisitor(tree sqlparser.Statement, queryInfo *QueryInfo) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch nodeType := node.(type) {
		case *sqlparser.Select:
			queryInfo.QueryType = SELECT
			return true, nil
		case *sqlparser.TableName:
			queryInfo.SelectFields = append(queryInfo.SelectFields, &SelectFieldExpression{
				TableName: node.(*sqlparser.TableName).Name.String(),
				ColName:   "",
			})
		case sqlparser.SelectExprs:
			q := node.(sqlparser.SelectExprs)
			for i := 0; i < len(q); i++ {
				var selExp SelectFieldExpression
				errSelect := SelectExprsVisitor(q[i], &selExp)
				if errSelect != nil {
					return false, errSelect
				}
				queryInfo.SelectFields = append(queryInfo.SelectFields, &selExp)
			}
			return false, nil
		//case *sqlparser.Union:

		//case sqlparser.Comments:
		// we don't care about comments
		case sqlparser.TableExprs:
			exps := node.(sqlparser.TableExprs)
			tblNames := make([]string, len(exps))
			for i, exp := range exps {
				err = TableExprVisitor(exp, &tblNames[i])
				if err != nil {
					return false, err
				}
			}
			queryInfo.JoinTables = tblNames
			return false, nil
		//case *sqlparser.Where:
		//case sqlparser.GroupBy:
		//case sqlparser.OrderBy:
		//case *sqlparser.Limit:

		default:
			return false, fmt.Errorf("RootSQLVisitor unsupported node type: %T", nodeType)
		}
		return false, nil
	}, tree)
}

func TableExprVisitor(node sqlparser.TableExpr, tableName *string) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch nodeType := node.(type) {
		case *sqlparser.AliasedTableExpr:
			return true, nil
		case sqlparser.TableName:
			*tableName = node.(sqlparser.TableName).Name.String()
			return false, nil
		case sqlparser.TableIdent:
			return false, nil
		//case *sqlparser.IndexHints:
		//	return false, nil
		default:
			return false, fmt.Errorf("TableExpr unsupported node type: %T", nodeType)
		}
	}, node)
}

func SelectExprsVisitor(node sqlparser.SQLNode, selectFieldExp *SelectFieldExpression) error {
	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
		switch nodeType := node.(type) {
		case *sqlparser.FuncExpr:
			selectFieldExp.IsAgg = true
			funcExp := node.(*sqlparser.FuncExpr)
			switch funcExp.Name.String() {
			case "count":
				selectFieldExp.AggType = COUNT_AGGREGATE
				return false, SelectExprsVisitor(funcExp.Exprs, selectFieldExp)
			default:
				return false, fmt.Errorf("unknown aggregation function: %s", funcExp.Name.String())
			}
		case *sqlparser.StarExpr:
			selectFieldExp.ColName = "*"
			return true, nil
		case sqlparser.TableName:
			selectFieldExp.TableName = node.(sqlparser.TableName).Name.String()
		case sqlparser.TableIdent:

		case sqlparser.ColIdent:
			//selectFieldExp.ColName =
		case sqlparser.SelectExpr:
		case sqlparser.SelectExprs:

		case *sqlparser.AliasedExpr:

		default:
			return false, fmt.Errorf("SelectExprsVisitor unsupported node type: %T", nodeType)
		}
		return true, nil
	}, node)
}

//func TableSchemaVisitor(node sqlparser.SQLNode, info *TableSchemaInfo) error {
//	parse, ok := node.(*sqlparser.DDL)
//	if !ok {
//		return fmt.Errorf("TableSchemaVisitor unsupported node type: %T", node)
//	}
//	if parse.Action != "create" {
//		return fmt.Errorf("TableSchemaVisitor unsupported action type: %T", parse.Action)
//	}
//	info.Name = parse.NewName.Name.String()
//
//	for i, column := range parse.TableSpec.Columns {
//		columnInfo := TableColumnInfo{
//			Idx:  i,
//			Name: "",
//			Type: 0,
//		}
//
//	}
//	return sqlparser.Walk(func(node sqlparser.SQLNode) (kontinue bool, err error) {
//		switch node.(type) {
//		case *sqlparser.TableSpec:
//		}
//		return true, nil
//	}, node)
//}

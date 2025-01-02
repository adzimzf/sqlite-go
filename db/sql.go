package db

import (
	"fmt"
	sql2 "github.com/adzimzf/sqlite-go/pkg/sql"
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
	JoinTables       []string         // SELECT
	WhereExpression_ *WhereExpression // SELECT, UPDATE, DELETE
	//LimitNum_            int32                    // SELECT
	//OffsetNum_           int32                    // SELECT
	//OrderByExpressions_  []*OrderByExpression     // SELECT
}

type WhereExpression struct {
	Operator string
	Left     string
	Right    string
}

func (q *QueryInfo) FieldNameByTable(tbName string) []string {
	var fieldNames []string
	for _, field := range q.SelectFields {
		fieldNames = append(fieldNames, field.ColName)
	}
	return fieldNames
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
	stmt, err := sql2.Parse(sql)
	if err != nil {
		return res, err
	}
	err = RootSQLVisitor(stmt, &res)
	if err != nil {
		return QueryInfo{}, err
	}

	return res, nil
}

func RootSQLVisitor(tree sql2.Statement, queryInfo *QueryInfo) error {
	return sql2.Walk(func(node sql2.SQLNode) (kontinue bool, err error) {
		switch nodeType := node.(type) {
		case *sql2.Select:
			queryInfo.QueryType = SELECT
			return true, nil
		case *sql2.TableName:
			queryInfo.SelectFields = append(queryInfo.SelectFields, &SelectFieldExpression{
				TableName: node.(*sql2.TableName).Name.String(),
				ColName:   "",
			})
		case sql2.SelectExprs:
			q := node.(sql2.SelectExprs)
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
		case *sql2.Where:
			n := node.(*sql2.Where)
			if n.Type != "where" {
				return false, nil
			}
			nc := n.Expr.(*sql2.ComparisonExpr)
			queryInfo.WhereExpression_ = &WhereExpression{
				Operator: nc.Operator,
			}
			lc := nc.Left.(*sql2.ColName)
			queryInfo.WhereExpression_.Left = lc.Name.String()
			rc := nc.Right.(*sql2.SQLVal)
			queryInfo.WhereExpression_.Right = string(rc.Val)
			return false, nil
		//case sqlparser.Comments:
		// we don't care about comments
		case sql2.TableExprs:
			exps := node.(sql2.TableExprs)
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

func TableExprVisitor(node sql2.TableExpr, tableName *string) error {
	return sql2.Walk(func(node sql2.SQLNode) (kontinue bool, err error) {
		switch nodeType := node.(type) {
		case *sql2.AliasedTableExpr:
			return true, nil
		case sql2.TableName:
			*tableName = node.(sql2.TableName).Name.String()
			return false, nil
		case sql2.TableIdent:
			return false, nil
		//case *sqlparser.IndexHints:
		//	return false, nil
		default:
			return false, fmt.Errorf("TableExpr unsupported node type: %T", nodeType)
		}
	}, node)
}

func SelectExprsVisitor(node sql2.SQLNode, selectFieldExp *SelectFieldExpression) error {
	return sql2.Walk(func(node sql2.SQLNode) (kontinue bool, err error) {
		switch nodeType := node.(type) {
		case *sql2.FuncExpr:
			selectFieldExp.IsAgg = true
			funcExp := node.(*sql2.FuncExpr)
			switch funcExp.Name.String() {
			case "count":
				selectFieldExp.AggType = COUNT_AGGREGATE
				return false, SelectExprsVisitor(funcExp.Exprs, selectFieldExp)
			default:
				return false, fmt.Errorf("unknown aggregation function: %s", funcExp.Name.String())
			}
		case *sql2.StarExpr:
			selectFieldExp.ColName = "*"
			return true, nil
		case sql2.TableName:
			selectFieldExp.TableName = node.(sql2.TableName).Name.String()
		case sql2.TableIdent:

		case sql2.ColIdent:
			//selectFieldExp.ColName =
		case sql2.SelectExpr:
		case sql2.SelectExprs:

		case *sql2.AliasedExpr:
		case *sql2.ColName:
			selectFieldExp.ColName = node.(*sql2.ColName).Name.String()

		default:
			return false, fmt.Errorf("SelectExprsVisitor unsupported node type: %T", nodeType)
		}
		return true, nil
	}, node)
}

func TableSchemaVisitor(node sql2.SQLNode, info *TableSchemaInfo) error {
	parse, ok := node.(*sql2.DDL)
	if !ok {
		return fmt.Errorf("TableSchemaVisitor unsupported node type: %T", node)
	}
	if parse.Action != "create" {
		return fmt.Errorf("TableSchemaVisitor unsupported action type: %T", parse.Action)
	}
	info.Name = parse.NewName.Name.String()

	for i, column := range parse.TableSpec.Columns {
		fieldType, found := StringToFieldType(column.Type.Type)
		if !found {
			return fmt.Errorf("TableSchemaVisitor unsupported column type: %T", column.Type.Type)
		}
		columnInfo := TableColumnInfo{
			Idx:  int64(i),
			Name: column.Name.String(),
			Type: fieldType,
		}

		if column.Type.KeyOpt == sql2.ColKeyPrimary {
			info.PrimaryKey = columnInfo
		}
		info.Columns = append(info.Columns, columnInfo)

	}
	return nil
}

package sql

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestListSupportedSQl(t *testing.T) {
	type TC struct {
		sql string
		st  Statement
	}

	tcs := []TC{
		{
			sql: "SELECT * FROM table_name",
			st: &Select{
				SelectExprs: []SelectExpr{
					&StarExpr{},
				},
				From: TableExprs{
					&AliasedTableExpr{
						Expr: TableName{
							Name: TableIdent{
								v: "table_name",
							},
						},
					},
				},
			},
		},
		{
			sql: "SELECT name as n FROM table_name AS tn",
			st: &Select{
				SelectExprs: []SelectExpr{
					&AliasedExpr{
						Expr: &ColName{
							Name: ColIdent{
								val:     "name",
								lowered: "",
							},
							Qualifier: TableName{},
						},
						As: ColIdent{
							val: "n",
						},
					},
				},
				From: TableExprs{
					&AliasedTableExpr{
						Expr: TableName{
							Name: TableIdent{
								v: "table_name",
							},
						},
						As: TableIdent{
							v: "tn",
						},
					},
				},
			},
		},
		{
			sql: "SELECT name as tn, nam FROM table_name as tn",
			st: &Select{
				SelectExprs: []SelectExpr{
					&AliasedExpr{
						Expr: &ColName{
							Name: ColIdent{
								val:     "name",
								lowered: "",
							},
							Qualifier: TableName{},
						},
						As: ColIdent{
							val: "tn",
						},
					},
					&AliasedExpr{
						Expr: &ColName{
							Metadata: nil,
							Name: ColIdent{
								val:     "nam",
								lowered: "",
							},
							Qualifier: TableName{},
						},
					},
				},
				From: TableExprs{
					&AliasedTableExpr{
						Expr: TableName{
							Name: TableIdent{
								v: "table_name",
							},
						},
						As: TableIdent{
							v: "tn",
						},
					},
				},
			},
		},

		{
			sql: "SELECT name as tn, nam, na FROM table_name as tn",
			st: &Select{
				SelectExprs: []SelectExpr{
					&AliasedExpr{
						Expr: &ColName{
							Name: ColIdent{
								val:     "name",
								lowered: "",
							},
							Qualifier: TableName{},
						},
						As: ColIdent{
							val: "tn",
						},
					},
					&AliasedExpr{
						Expr: &ColName{
							Metadata: nil,
							Name: ColIdent{
								val:     "nam",
								lowered: "",
							},
							Qualifier: TableName{},
						},
					},
					&AliasedExpr{
						Expr: &ColName{
							Metadata: nil,
							Name: ColIdent{
								val:     "na",
								lowered: "",
							},
							Qualifier: TableName{},
						},
					},
				},
				From: TableExprs{
					&AliasedTableExpr{
						Expr: TableName{
							Name: TableIdent{
								v: "table_name",
							},
						},
						As: TableIdent{
							v: "tn",
						},
					},
				},
			},
		},
		{
			sql: "SELECT name.* FROM table_name as tn",
			st: &Select{
				SelectExprs: []SelectExpr{
					&AliasedExpr{
						Expr: &ColName{
							Name: ColIdent{
								val:     "*",
								lowered: "",
							},
							Qualifier: TableName{
								Name: TableIdent{
									v: "name",
								},
								Qualifier: TableIdent{},
							},
						},
					},
				},
				From: TableExprs{
					&AliasedTableExpr{
						Expr: TableName{
							Name: TableIdent{
								v: "table_name",
							},
						},
						As: TableIdent{
							v: "tn",
						},
					},
				},
			},
		},
		{
			sql: "CREATE TABLE apples ( id integer primary key autoincrement, name text, color text)",
			st: &DDL{
				Action: "create",
				NewName: TableName{
					Name: TableIdent{
						v: "apples",
					},
					Qualifier: TableIdent{},
				},
				IfExists: false,
				TableSpec: &TableSpec{
					Columns: []*ColumnDefinition{
						{
							Name: ColIdent{
								val:     "id",
								lowered: "",
							},
							Type: ColumnType{
								Type:          "INTEGER",
								Autoincrement: true,
								KeyOpt:        ColKeyPrimary,
							},
						},
						{
							Name: ColIdent{
								val:     "name",
								lowered: "",
							},
							Type: ColumnType{
								Type: "TEXT",
							},
						},
						{
							Name: ColIdent{
								val:     "color",
								lowered: "",
							},
							Type: ColumnType{
								Type: "TEXT",
							},
						},
					},
					Options: "",
				},
				VindexCols: nil,
			},
		},

		{
			sql: "SELECT COUNT(*) as col FROM table_name as tn",
			st: &Select{
				Cache:    "",
				Distinct: "",
				Hints:    "",
				SelectExprs: []SelectExpr{
					&AliasedExpr{
						Expr: &FuncExpr{
							Name: ColIdent{
								val: "count",
							},
							Exprs: SelectExprs{
								&StarExpr{},
							},
						},
						As: ColIdent{
							val: "col",
						},
					},
				},
				From: TableExprs{
					&AliasedTableExpr{
						Expr: TableName{
							Name: TableIdent{
								v: "table_name",
							},
						},
						As: TableIdent{
							v: "tn",
						},
					},
				},
				Lock: "",
			},
		},
	}
	//supportedSQL := []string{
	//,
	//"SELECT name as n FROM table_name",
	//"",
	//"",
	//"SELECT name.*, nam FROM table_name",

	//"SELECT COUNT(*) as col FROM table_name",
	//"SELECT SUM(table_id.column) FROM table_name",
	//"SELECT COUNT(*),SUM(column_id) FROM table_name",
	//"CREATE TABLE apples\n ( \nid integer primary key autoincrement,\nname text,\ncolor text)",
	//}

	for _, tc := range tcs {
		t.Run(tc.sql, func(t *testing.T) {
			p, err := Parse(tc.sql)
			require.NoError(t, err)
			require.NotNil(t, p)
			require.Equal(t, tc.st, p)
		})
	}
}

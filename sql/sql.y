%{
package sql

func setParseTree(yylex interface{}, stmt Statement) {
  yylex.(*Lexer).ParseTree = stmt
}

func setDDL(yylex interface{}, ddl *DDL) {
  yylex.(*Lexer).partialDDL = ddl
}

%}

%union {
 empty         struct{}
 statement     Statement
 selStmt       SelectStatement
 selectExprs   SelectExprs
 selectExpr    SelectExpr
 expr          Expr
 colName       *ColName
 colIdent      ColIdent
 tableIdent    TableIdent
 bytes         []byte
 tableExprs    TableExprs
 tableExpr     TableExpr
 tableName     TableName
 tableNames    TableNames
 TableSpec     *TableSpec
 aliasedTableName *AliasedTableExpr
 columnDefinition *ColumnDefinition
 columnType    ColumnType
 colKeyOpt     ColumnKeyOption
 ddl           *DDL
 optVal        *SQLVal
 boolVal       BoolVal

 string        string
 str           string
}

%type <statement> command
%type <statement> create_statement
%type <selStmt> select_statement
%type <selStmt> base_select
%type <selectExprs> select_expression_list
%type <selectExpr> select_expression
%type <expr> expression
%type <expr> value
%type <expr> value_expression
%type <expr> function_call_keyword
%type <expr> condition
%type <expr> where_expression_opt
%type <str> compare
%type <colName> column_name
%type <colIdent> sql_id
%type <colIdent> reserved_sql_id
%type <colIdent> as_ci_opt
%type <colIdent> col_alias
%type <tableIdent> table_id
%type <tableIdent> table_alias
%type <tableIdent> as_opt_id
%type <tableExprs> from_opt
%type <tableExprs> table_references
%type <tableExpr> table_reference
%type <tableExpr> table_factor
%type <tableName> table_name
%type <aliasedTableName> aliased_table_name
%type <columnDefinition> column_definition
%type <ddl> create_table_prefix
%type <TableSpec> table_spec
%type <TableSpec> table_column_list
%type <columnType> column_type
%type <optVal> column_default_opt
%type <optVal> on_update_opt
%type <boolVal> autoincrement_opt
%type <colKeyOpt> column_key_opt

%nonassoc <bytes> '.'

%token SELECT
%token FROM
%token COMMA
%token AS
%token LPAREN
%token RPAREN
%token COUNT
%token WHERE
%token AND
%token OR
%token NOT
%token EQUAL
%token <string> INTEGER
%token <string> TEXT
%token <string> BLOB
%token CREATE
%token PRIMARY
%token KEY
%token AUTOINCREMENT
%token TABLE
%token <str> STAR
%token <str> IDENTIFIER
%token <str> STRING

%start any_command

%left <bytes> OR
%left <bytes> AND
%right <bytes> NOT '!'
%left <bytes>  '<' '>'

%%
any_command:
  command semicolon_opt
  {
    setParseTree(yylex, $1)
  }

semicolon_opt:
/*empty*/ {}
| ';' {}


command:
  select_statement
  {
    $$ = $1
  }
  | create_statement


select_statement:
    base_select
    {
        sel := $1.(*Select)
        $$ = sel
    }

base_select:
    SELECT select_expression_list from_opt where_expression_opt
    {
        $$ = &Select{ SelectExprs: $2, From: $3, Where: NewWhere(WhereStr, $4)}
    }

select_expression_list:
    select_expression
    {
        $$ = SelectExprs{$1}
    }
    |
    select_expression_list COMMA select_expression
    {
         $$ = append($$, $3)
    }

select_expression:
    STAR {
        $$ = &StarExpr{}
    }
    | expression as_ci_opt
    {
       $$ = &AliasedExpr{Expr: $1, As: $2}
    }

expression:
    value_expression
    {
       $$ = $1
    }

value_expression:
  value
  {
    $$ = $1
  }
  |
  column_name
  {
    $$ = $1
  }
  |
  function_call_keyword
  {
    $$ = $1
  }

condition:
  value_expression compare value_expression
  {
    $$ = &ComparisonExpr{Left: $1, Operator: $2, Right: $3}
  }

value:
  STRING
  {
    $$ = NewStrVal([]byte($1))
  }

compare:
  EQUAL
  {
    $$ = EqualStr
  }
  | '<'
  {
    $$ = LessThanStr
  }
  | '>'
  {
    $$ = GreaterThanStr
  }

column_name:
  sql_id
  {
    $$ = &ColName{Name: $1}
  }
  | table_id '.' reserved_sql_id
  {
    $$ = &ColName{Qualifier: TableName{Name: $1}, Name: $3}
  }

sql_id:
  IDENTIFIER
  {
    $$ = NewColIdent(string($1))
  }


table_id:
  IDENTIFIER
  {
    $$ = NewTableIdent(string($1))
  }

reserved_sql_id:
  STAR
  {
   $$ = NewColIdent(string('*'))
  }
  |
  sql_id
  {
    $$ = $1
  }

as_ci_opt:
  {
    $$ = ColIdent{}
  }
  | col_alias
  {
    $$ = $1
  }
  | AS col_alias
  {
    $$ = $2
  }

col_alias:
  sql_id
  {
    $$ = $1
  }

function_call_keyword:
  COUNT LPAREN select_expression RPAREN
  {
     $$ = &FuncExpr{Name: NewColIdent("count"), Exprs: NewSelectExprs($3)}
  }

from_opt:
  {
    $$ = TableExprs{&AliasedTableExpr{Expr:TableName{Name: NewTableIdent("dual")}}}
  }
  | FROM table_references
  {
    $$ = $2
  }

where_expression_opt:
  {
    $$ = nil
  }
  |
  WHERE expression
  {
    $$ = $2
  }

expression:
   condition
   {
     $$ = $1
   }
   |
   expression AND expression
   {
     $$ = &AndExpr{Left: $1, Right: $3}
   }
   |
   expression OR expression
   {
     $$ = &OrExpr{Left: $1, Right: $3}
   }
   |
   NOT expression
   {
     $$ = &NotExpr{Expr: $2}
   }

table_references:
  table_reference
  {
    $$ = TableExprs{$1}
  }
  | table_references COMMA table_reference
  {
    $$ = append($$, $3)
  }

table_reference:
  table_factor
  {
    $$ = $1
  }

table_factor:
  aliased_table_name
  {
    $$ = $1
  }

aliased_table_name:
  table_name as_opt_id
  {
    $$ = &AliasedTableExpr{Expr:$1, As: $2}
  }

table_name:
  table_id
  {
    $$ = TableName{Name: $1}
  }

as_opt_id:
  {
    $$ = NewTableIdent("")
  }
  | table_alias
  {
    $$ = $1
  }
  | AS table_alias
  {
    $$ = $2
  }

table_alias:
  table_id
  {
    $$ = $1
  }

create_statement:
  create_table_prefix table_spec
  {
    $1.TableSpec = $2
    $$ = $1
  }

create_table_prefix:
  CREATE TABLE table_name
  {
    $$ = &DDL{Action: CreateStr, NewName: $3}
    setDDL(yylex, $$)
  }

table_spec:
  LPAREN table_column_list RPAREN
  {
    $$ = $2
  }

table_column_list:
  column_definition
  {
    $$ = &TableSpec{}
    $$.AddColumn($1)
  }
  |
  table_column_list COMMA column_definition
  {
    $$.AddColumn($3)
  }

column_definition:
  IDENTIFIER column_type column_default_opt on_update_opt column_key_opt autoincrement_opt
  {
    $2.Default = $3
    $2.OnUpdate = $4
    $2.KeyOpt = $5
    $2.Autoincrement = $6
    $$ = &ColumnDefinition{Name: NewColIdent(string($1)), Type: $2}
  }

column_type:
   INTEGER
   {
     $$ = NewIntegerColumn()
   }
   | TEXT
   {
     $$ = NewTextColumn()
   }
   | BLOB
   {
     $$ = NewBlobColumn()
   }

column_default_opt:
  {
    $$ = nil
  }

on_update_opt:
  {
    $$ = nil
  }

column_key_opt:
  {
    $$ = ColKeyNone
  }
  | PRIMARY KEY
  {
    $$ = ColKeyPrimary
  }

autoincrement_opt:
  {
    $$ = BoolVal(false)
  }
  |
  AUTOINCREMENT
  {
    $$ = BoolVal(true)
  }

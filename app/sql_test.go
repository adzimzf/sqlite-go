package main

import (
	sqlparser "github.com/adzimzf/sqlite-go/sql"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTableSchemaVisitor(t *testing.T) {
	type args struct {
		node sqlparser.SQLNode
		info *TableSchemaInfo
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "",
			args: args{
				node: func() sqlparser.SQLNode {
					sql := "CREATE TABLE apples\n(\n\tid integer primary key autoincrement,\n\tname text,\n\tcolor text\n)"
					statement, err := sqlparser.Parse(sql)
					require.NoError(t, err)
					return statement
				}(),
				info: nil,
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := TableSchemaVisitor(tt.args.node, tt.args.info); (err != nil) != tt.wantErr {
				t.Errorf("TableSchemaVisitor() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

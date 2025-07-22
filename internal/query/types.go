package query

type ColumnSchema struct {
	Name string
	Type string
}

type ColumnVal struct {
	Name string
	Val  string
}

type CreateOp struct {
	Table  string
	Schema []ColumnSchema
}

type InsertOp struct {
	Table  string
	Values []ColumnVal
}

type AstNode struct {
	Op string

	ColumnName string
	Value      string

	SubOp1 *AstNode
	SubOp2 *AstNode
}

type WhereAST struct {
	ast *AstNode
}

type SelectOp struct {
	Table   string
	Columns []string
	where   *WhereAST
}

type Query interface{}

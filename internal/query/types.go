package query

type JSONString string

type Schema map[string]interface{}
type Value map[string]interface{}

type ColumnVal struct {
	Name string
	Val  string
}

type CreateOp struct {
	Document string
	Schema   Schema
}

type InsertOp struct {
	Document string
	Value    Value
}

type SelectOp struct {
	Document string
	Columns  []string
	ID       int64
}

type Query interface{}

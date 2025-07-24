package query

// JSONString represents a raw JSON string input.
type JSONString string

// Schema defines the structure of a document, mapping field names to types.
type Schema map[string]interface{}

// Value represents the actual data to be inserted, as field-value pairs.
type Value map[string]interface{}

// ColumnVal represents a single column and its value, used in queries.
type ColumnVal struct {
	Name string
	Val  string
}

// CreateOp represents a parsed CREATE DOCUMENT operation.
type CreateOp struct {
	Document string
	Schema   Schema
}

// InsertOp represents a parsed INSERT VALUE INTO operation.
type InsertOp struct {
	Document string
	Value    Value
}

// SelectOp represents a parsed SELECT ... FROM ... WITH _ID= operation.
type SelectOp struct {
	Document string
	Columns  []string
	ID       int64
}

// Query is a generic interface for all query operation types (CreateOp, InsertOp, etc.).
type Query interface{}

package errors

import (
	"fmt"
)

// SQLError represents a general SQL error.
type SQLError string

func (e SQLError) Error() string {
	return fmt.Sprintf("SQL error: %s", string(e))
}

func SQLParseError(msg string, args ...any) error {
	return SQLError(fmt.Sprintf("parse error: "+msg, args...))
}

func SQLSyntaxError(msg string, args ...any) error {
	return SQLError(fmt.Sprintf("syntax error: "+msg, args...))
}

// SchemaError represents a schema-related error.
type SchemaError string

func (e SchemaError) Error() string {
	return fmt.Sprintf("Schema error: %s", string(e))
}

func SchemaValidationError(msg string, args ...any) error {
	return SchemaError(fmt.Sprintf("validation error: "+msg, args...))
}

func SchemaJSONMarshallError(msg string, args ...any) error {
	return SchemaError(fmt.Sprintf("JSON marshall error: "+msg, args...))
}

func SchemaJSONUnmarshallError(msg string, args ...any) error {
	return SchemaError(fmt.Sprintf("JSON unmarshall error: "+msg, args...))
}

func DuplicateSchemaError(msg string, args ...any) error {
	return SchemaError(fmt.Sprintf("duplicate schema: "+msg, args...))
}

func TypeCastError(msg string, args ...any) error {
	return SchemaError(fmt.Sprintf("typecast failed: "+msg, args...))
}

func UnknownField(msg string, args ...any) error {
	return SchemaError(fmt.Sprintf("unknown field: "+msg, args...))
}

func MissingFields(msg string, args ...any) error {
	return SchemaError(fmt.Sprintf("missing fields: "+msg, args...))
}

// DBError represents a general database error.
type DBError string

func (e DBError) Error() string {
	return fmt.Sprintf("Database error: %s", string(e))
}

func InsertError(msg string, args ...any) error {
	return DBError(fmt.Sprintf("insert error: "+msg, args...))
}

func SelectError(msg string, args ...any) error {
	return DBError(fmt.Sprintf("select error: "+msg, args...))
}

func DeleteError(msg string, args ...any) error {
	return DBError(fmt.Sprintf("select error: "+msg, args...))
}

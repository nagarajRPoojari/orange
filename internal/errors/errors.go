package errors

import (
	"fmt"
)

type SQLError string

func (t SQLError) Error() string {
	return fmt.Sprintf("SQL error: %s", string(t))
}

func SQLParseError(msg string) error {
	return SQLError("SQL parse error: " + msg)
}

func SQLSyntaxError(msg string) error {
	return SQLError("SQL syntax error: " + msg)
}

type SchemaError string

func (t SchemaError) Error() string {
	return fmt.Sprintf("Schema error: %s", string(t))
}

func SchemaValidationError(msg string) error {
	return SchemaError("validation error: " + msg)
}

func SchemaJSONMarshallError(msg string) error {
	return SchemaError(msg)
}

func SchemaJSONUnMarshallError(msg string) error {
	return SchemaError(msg)
}

func DuplicateSchemaError(msg string) error {
	return SchemaError("duplicate schema found: " + msg)
}

func TypeCastError(msg string) error {
	return SchemaError("failed to typecast: " + msg)
}

type DBError string

func (t DBError) Error() string {
	return fmt.Sprintf("Schema error: %s", string(t))
}

func InsertError(msg string) error {
	return DBError("insert error: " + msg)
}

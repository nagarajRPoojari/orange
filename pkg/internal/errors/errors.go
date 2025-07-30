package errors

import "fmt"

// OQLError represents a general OQL error.
type OQLError string

func (e OQLError) Error() string {
	return fmt.Sprintf("OQL error: %s", string(e))
}

func OQLParseError(msg string, args ...any) error {
	return OQLError(fmt.Sprintf("parse error: "+msg, args...))
}

func OQLSyntaxError(msg string, args ...any) error {
	return OQLError(fmt.Sprintf("syntax error: "+msg, args...))
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

func UnknownField(msg string, args ...any) error {
	return SchemaError(fmt.Sprintf("unknown field: "+msg, args...))
}

func MissingFields(msg string, args ...any) error {
	return SchemaError(fmt.Sprintf("missing fields: "+msg, args...))
}

func TypeCastError(msg string, args ...any) error {
	return SchemaError(fmt.Sprintf("missing fields: "+msg, args...))
}

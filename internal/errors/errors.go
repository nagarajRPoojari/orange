package errors

import (
	"fmt"
)

type TypeCastError string

func (e TypeCastError) Error() string {
	return fmt.Sprintf("Schema error: %s", string(e))
}

func RaiseTypeCastErr(msg string, args ...any) error {
	return TypeCastError(fmt.Sprintf("typecast failed: "+msg, args...))
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

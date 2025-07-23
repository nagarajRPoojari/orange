package errors

import "fmt"

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

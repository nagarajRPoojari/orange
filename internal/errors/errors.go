package errors

import "fmt"

const SQLParseError = SQLError("SQL parsing error")
const SQLSyntaxError = SQLError("SQL syntax error")

type SQLError string

func (t SQLError) Error() string {
	return fmt.Sprintf("io err: %s", string(t))
}

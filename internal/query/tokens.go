package query

type TokenType string

const (
	// Keywords
	T_CREATE TokenType = "CREATE"
	T_TABLE  TokenType = "TABLE"
	T_INSERT TokenType = "INSERT"
	T_INTO   TokenType = "INTO"
	T_VALUES TokenType = "VALUES"
	T_SELECT TokenType = "SELECT"
	T_FROM   TokenType = "FROM"
	T_WHERE  TokenType = "WHERE"

	// Symbols
	T_LPAREN    TokenType = "("
	T_RPAREN    TokenType = ")"
	T_COMMA     TokenType = ","
	T_SEMICOLON TokenType = ";"
	T_ASTERISK  TokenType = "*"

	// OPS
	T_LESSTHAN            TokenType = "<"
	T_LESSTHANOREQALS     TokenType = "<="
	T_GREATERTHAN         TokenType = ">"
	T_GREATERTHANOREQUALS TokenType = ">="
	T_EQUALS              TokenType = "="
	T_NOTEQUALS           TokenType = "!="
	T_AND                 TokenType = "AND"
	T_OR                  TokenType = "OR"

	// Literals
	T_IDENTIFIER TokenType = "IDENTIFIER"
	T_INT        TokenType = "INT"
	T_STRING     TokenType = "STRING"

	// Special
	T_EOF TokenType = "EOF"
)

type Token struct {
	Type  TokenType
	Value string
}

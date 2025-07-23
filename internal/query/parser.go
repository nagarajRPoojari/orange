package query

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/nagarajRPoojari/orange/internal/errors"
)

type Parser struct {
	input string
}

func NewParser(input string) *Parser {
	return &Parser{input}
}

func (t *Parser) Build() (Query, error) {
	return t.parse()

}

func (t Parser) parse() (Query, error) {
	input := strings.TrimSpace(strings.ToUpper(t.input))

	switch {
	case strings.HasPrefix(input, string(T_SELECT)):
		return t.ParseSelectQuery()
	case strings.HasPrefix(input, string(T_INSERT)):
		return t.ParseInsertQuery()
	case strings.HasPrefix(input, string(T_CREATE)):
		return t.ParseCreateQuery()
	default:
		return nil, errors.SQLSyntaxError("unsupported statement")
	}
}

func extractOutermost(input string, startChar, endChar rune) (string, error) {
	start := regexp.QuoteMeta(string(startChar))
	end := regexp.QuoteMeta(string(endChar))

	pattern := fmt.Sprintf("%s(.*)%s", start, end)
	re := regexp.MustCompile(pattern)

	match := re.FindStringSubmatch(input)
	if len(match) > 1 {
		return match[1], nil
	}
	return "", errors.SQLSyntaxError("failed to extract fields")
}

//	CREATE DOCUMENT user {
//	 name: "hello",
//		game: "hello"
//	}
func (t *Parser) ParseCreateQuery() (CreateOp, error) {
	doc, err := extractOutermost(t.input, '{', '}')
	if err != nil {
		var null CreateOp
		return null, err
	}
	name, err := extractDocumentNameFromCreateQuery(t.input)
	if err != nil {
		var null CreateOp
		return null, err
	}

	schema, err := unmarshallSchema(JSONString(doc))
	if err != nil {
		var null CreateOp
		return null, err
	}

	return CreateOp{
		Document: name,
		Schema:   schema,
	}, nil
}

func extractDocumentNameFromCreateQuery(input string) (string, error) {
	re := regexp.MustCompile(fmt.Sprintf(`(?i)%s\s+%s\s+(\w+)`, T_CREATE, T_DOCUMENT))
	match := re.FindStringSubmatch(input)
	if len(match) > 1 {
		return match[1], nil
	}
	return "", errors.SQLSyntaxError("failed to extract document name")
}

func unmarshallSchema(input JSONString) (Schema, error) {
	var schema Schema
	err := json.Unmarshal([]byte("{"+input+"}"), &schema)
	if err != nil {
		return nil, errors.SQLSyntaxError("invalid JSON schema: " + err.Error())
	}
	return schema, nil
}

// INSERT VALUE INTO user {
//
// }
func (t *Parser) ParseInsertQuery() (InsertOp, error) {
	doc, err := extractOutermost(t.input, '{', '}')
	if err != nil {
		var null InsertOp
		return null, err
	}
	name, err := extractDocumentNameFromInsertQuery(t.input)
	if err != nil {
		var null InsertOp
		return null, err
	}

	value, err := unmarshallValue(JSONString(doc))
	if err != nil {
		var null InsertOp
		return null, err
	}

	return InsertOp{
		Document: name,
		Value:    value,
	}, nil
}

func extractDocumentNameFromInsertQuery(input string) (string, error) {
	re := regexp.MustCompile(fmt.Sprintf(`(?i)%s\s+%s\s+%s\s+(\w+)`, T_INSERT, T_VALUE, T_INTO))
	match := re.FindStringSubmatch(input)
	if len(match) > 1 {
		return match[1], nil
	}
	return "", errors.SQLSyntaxError("failed to extract document name")
}

func unmarshallValue(input JSONString) (Value, error) {
	var schema Value
	err := json.Unmarshal([]byte("{"+input+"}"), &schema)
	if err != nil {
		return nil, errors.SQLSyntaxError("invalid JSON schema: " + err.Error())
	}
	return schema, nil
}

// SELECT name, age FROM user WITH _ID="some_key"
func (t *Parser) ParseSelectQuery() (SelectOp, error) {
	name, err := extractDocumentNameFromSelectQuery(t.input)
	if err != nil {
		var null SelectOp
		return null, err
	}

	_id, err := extractID(t.input)
	if err != nil {
		var null SelectOp
		return null, err
	}

	cols := extractColumnNames(t.input)
	return SelectOp{
		Document: name,
		Columns:  cols,
		_ID:      _id,
	}, nil
}

func extractID(input string) (string, error) {
	re := regexp.MustCompile(`_ID\s*=\s*"(.*?)"`)
	match := re.FindStringSubmatch(input)
	if len(match) > 1 {
		return match[1], nil
	}
	return "", errors.SQLSyntaxError("failed to extract _ID")
}

func extractColumnNames(input string) []string {
	re := regexp.MustCompile(fmt.Sprintf(`(?i)%s\s+(.+?)\s+%s`, T_SELECT, T_FROM))
	match := re.FindStringSubmatch(input)
	if len(match) > 1 {
		rawFields := match[1]
		parts := strings.Split(rawFields, ",")
		var fields []string
		for _, field := range parts {
			fields = append(fields, strings.TrimSpace(field))
		}
		return fields
	}
	return nil
}

func extractDocumentNameFromSelectQuery(input string) (string, error) {
	re := regexp.MustCompile(`(?i)FROM\s+(\w+)`)
	match := re.FindStringSubmatch(input)
	if len(match) > 1 {
		return match[1], nil
	}
	return "", errors.SQLSyntaxError("failed to extract document name")
}

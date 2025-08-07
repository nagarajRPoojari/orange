package oql

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/nagarajRPoojari/orange/pkg/internal/errors"
)

type Parser struct {
	input string
}

func NewParser(input string) *Parser {
	return &Parser{input}
}

// Build parses query and builds respective QueryOp
//   - supports INSERT, CREATE, SELECT queries
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
	case strings.HasPrefix(input, string(T_DELETE)):
		return t.ParseDeleteQuery()
	default:
		return nil, errors.OQLSyntaxError("unsupported statement")
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
	return "", errors.OQLSyntaxError("failed to extract fields")
}

// ParseCreateQuery parses a CREATE DOCUMENT query and returns a CreateOp.
//
// Expected format:
//
//	CREATE DOCUMENT <name> { "field1": "type1", "field2": "type2", ... }
//
// It extracts the document name and schema definition, returning them
// as a structured CreateOp. Returns an error if parsing fails.
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
	return "", errors.OQLSyntaxError("failed to extract document name")
}

func unmarshallSchema(input JSONString) (Schema, error) {
	var schema Schema
	err := json.Unmarshal([]byte("{"+input+"}"), &schema)
	if err != nil {
		return nil, errors.OQLSyntaxError("invalid JSON schema: " + err.Error())
	}
	return schema, nil
}

// ParseInsertQuery parses an INSERT VALUE query and returns an InsertOp.
//
// Expected format:
//
//	INSERT VALUE INTO <document> {
//	  "field1": <value1>,
//	  "field2": <value2>,
//	  ...
//	}
//
// It extracts the target document name and the JSON payload to insert.
// Returns an error if parsing or unmarshalling fails.
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
	return "", errors.OQLSyntaxError("failed to extract document name")
}

func unmarshallValue(input JSONString) (Value, error) {
	var schema Value
	err := json.Unmarshal([]byte("{"+input+"}"), &schema)
	if err != nil {
		return nil, errors.OQLSyntaxError("invalid JSON schema: " + err.Error())
	}
	return schema, nil
}

// ParseSelectQuery parses a SELECT query and returns a SelectOp.
//
// Expected format:
//
//	SELECT <columns> FROM <document> WITH _ID=<key>
//
// Extracts the document name, selected columns, and required _ID filter.
// Returns an error if parsing fails or if _ID is missing.
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
		ID:       _id,
	}, nil
}

func extractID(input string) (int64, error) {
	re := regexp.MustCompile(`_ID\s*=\s*(\d+)`)
	match := re.FindStringSubmatch(input)
	if len(match) > 1 {
		numStr := strings.TrimSpace(match[1])
		num, err := strconv.ParseInt(numStr, 10, 64)
		if err != nil {
			return 0, errors.OQLSyntaxError("failed to parse _ID to int64")
		}
		return num, nil
	}
	return 0, errors.OQLSyntaxError("failed to extract _ID")
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
	return "", errors.OQLSyntaxError("failed to extract document name")
}

// ParseSelectQuery parses a DELETE query and returns a SelectOp.
//
// Expected format:
//
//	DELETE DOCUMENT FROM <document> WHERE _ID=<key>
//
// Returns an error if parsing fails or if _ID is missing.
func (t *Parser) ParseDeleteQuery() (DeleteOp, error) {
	var null DeleteOp
	name, err := extractDocumentNameFromDeleteQuery(t.input)
	if err != nil {
		return null, err
	}

	_id, err := extractID(t.input)
	if err != nil {
		return null, err
	}
	return DeleteOp{
		Document: name,
		ID:       _id,
	}, nil
}

func extractDocumentNameFromDeleteQuery(input string) (string, error) {
	re := regexp.MustCompile(`(?i)DELETE\s+DOCUMENT\s+FROM\s+(\w+)\s+WHERE\s+_ID\s*=\s*[\w\d]+`)
	match := re.FindStringSubmatch(input)
	if len(match) > 1 {
		return match[1], nil
	}
	return "", errors.OQLSyntaxError("failed to extract document name from DELETE query")
}

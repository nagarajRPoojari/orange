package query

import (
	"testing"

	"github.com/nagarajRPoojari/orange/internal/errors"
	"github.com/stretchr/testify/assert"
)

func TestCreateParser(t *testing.T) {
	createQ := `CREATE DOCUMENT users { "_ID": {"auto_increment": false},"name": "string", "age": {"name": "string"} }`
	expectedCreate := CreateOp{
		Document: "users",
		Schema: map[string]interface{}{
			"_ID": map[string]interface{}{
				"auto_increment": false,
			},
			"name": "string",
			"age": map[string]interface{}{
				"name": "string",
			},
		},
	}
	got, err := NewParser(createQ).Build()
	assert.NoError(t, err, assert.AnError)
	assert.Equal(t, expectedCreate, got)
}

func TestInsertParser(t *testing.T) {
	insertQ := `INSERT VALUE INTO users {"age": 1, "name": "Alice", "score": 30, "interest": { "food": "cake", "name":89000}}`
	expectedInsert := InsertOp{
		Document: "users",
		Value: map[string]interface{}{
			"age":   float64(1), // JSON numbers decode as float64 by default
			"name":  "Alice",
			"score": float64(30),
			"interest": map[string]interface{}{
				"food": "cake",
				"name": float64(89000),
			},
		},
	}

	got, err := NewParser(insertQ).Build()
	assert.NoError(t, err, assert.AnError)
	assert.Equal(t, expectedInsert, got)
}

func TestInvalidQuery(t *testing.T) {
	insertQ := "INSERT INO users (id, name, age, team) VALUES (1, 'Alice', 30, 90)"

	_, err := NewParser(insertQ).Build()
	assert.ErrorIs(t, err, errors.SQLSyntaxError("failed to extract fields"))
}

func TestSelectParser(t *testing.T) {
	selectQ := `SELECT name.game, abc FROM users WHERE _ID=263920338392`
	expectedInsert := SelectOp{
		Document: "users",
		Columns:  []string{"name.game", "abc"},
		_ID:      263920338392,
	}

	got, err := NewParser(selectQ).Build()
	assert.NoError(t, err, assert.AnError)
	assert.Equal(t, expectedInsert, got)
}

package query

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCreateParser(t *testing.T) {
	createQ := "CREATE TABLE users (id INT, name TEXT, age INT, card INT, sr VARCHAR)"
	expected := CreateOp{
		Table:  "users",
		Schema: []ColumnSchema{{Name: "id", Type: "int"}, {Name: "name", Type: "text"}, {Name: "age", Type: "int"}, {Name: "card", Type: "int"}, {Name: "sr", Type: "varchar"}},
	}

	got, err := NewParser(createQ).Build()
	assert.NotErrorIs(t, err, assert.AnError)
	assert.Equal(t, expected, got)
}

func TestInsertParser(t *testing.T) {
	insertQ := "INSERT INTO users (id, name, age, team) VALUES (1, 'Alice', 30, 90)"
	expected := InsertOp{
		Table: "users",
		Values: []ColumnVal{
			{Name: "id", Val: "1"},
			{Name: "name", Val: "'Alice'"},
			{Name: "age", Val: "30"},
			{Name: "team", Val: "90"},
		},
	}

	got, err := NewParser(insertQ).Build()
	assert.NotErrorIs(t, err, assert.AnError)
	assert.Equal(t, expected, got)
}

func TestSelectParser(t *testing.T) {
	query := "SELECT name, abc FROM users WHERE (name = 'abc' OR b = 91 OR (age = 90 AND x >= 90))"

	expected := SelectOp{
		Table:   "users",
		Columns: []string{"name", "abc"},
		where: &WhereAST{
			ast: &AstNode{
				Op: string(T_OR),
				SubOp1: &AstNode{
					Op: string(T_OR),
					SubOp1: &AstNode{
						Op:     string(T_EQUALS),
						SubOp1: &AstNode{ColumnName: "name"},
						SubOp2: &AstNode{Value: "abc"},
					},
					SubOp2: &AstNode{
						Op:     string(T_EQUALS),
						SubOp1: &AstNode{ColumnName: "b"},
						SubOp2: &AstNode{Value: "91"},
					},
				},
				SubOp2: &AstNode{
					Op: string(T_AND),
					SubOp1: &AstNode{
						Op:     string(T_EQUALS),
						SubOp1: &AstNode{ColumnName: "age"},
						SubOp2: &AstNode{Value: "90"},
					},
					SubOp2: &AstNode{
						Op:     string(T_GREATERTHANOREQUALS),
						SubOp1: &AstNode{ColumnName: "x"},
						SubOp2: &AstNode{Value: "90"},
					},
				},
			},
		},
	}

	got, err := NewParser(query).Build()
	assert.NotErrorIs(t, err, assert.AnError)
	assert.Equal(t, expected, got)
}

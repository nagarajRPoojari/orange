package oql

import (
	"reflect"
	"testing"
)

func TestParser_ParseCreateQuery(t *testing.T) {
	type fields struct {
		input string
	}
	tests := []struct {
		name    string
		fields  fields
		want    CreateOp
		wantErr bool
	}{
		{
			name: "valid create query",
			fields: fields{
				input: `CREATE DOCUMENT users { "_ID": {"auto_increment": false},"name": "string", "age": {"name": "string"} }`,
			},
			want: CreateOp{
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
			},
			wantErr: false,
		},
		{
			name: "invalid create query",
			fields: fields{
				input: `CREATE DOCUMENT users { "_ID": {"auto_increment": false},"name": "string", "age": { }`,
			},
			want:    CreateOp{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Parser{
				input: tt.fields.input,
			}
			got, err := tr.ParseCreateQuery()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parser.ParseCreateQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parser.ParseCreateQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParser_ParseInsertQuery(t *testing.T) {
	type fields struct {
		input string
	}
	tests := []struct {
		name    string
		fields  fields
		want    InsertOp
		wantErr bool
	}{
		{
			name: "valid insert query",
			fields: fields{
				input: `INSERT VALUE INTO users {"age": 1, "name": "Alice", "score": 30, "interest": { "food": "cake", "name":89000}}`,
			},
			want: InsertOp{
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
			},
			wantErr: false,
		},
		{
			name: "valid insert query",
			fields: fields{
				input: `INSERT VALUE I users {"age": 1, "name": "Alice", "score": 30, "interest": { "food": "cake", "name":89000}}`,
			},
			want:    InsertOp{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Parser{
				input: tt.fields.input,
			}
			got, err := tr.ParseInsertQuery()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parser.ParseInsertQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parser.ParseInsertQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParser_ParseSelectQuery(t *testing.T) {
	type fields struct {
		input string
	}
	tests := []struct {
		name    string
		fields  fields
		want    SelectOp
		wantErr bool
	}{
		{
			name: "valid select query",
			fields: fields{
				input: `SELECT name.game, abc FROM users WHERE _ID=263920338392`,
			},
			want: SelectOp{
				Document: "users",
				Columns:  []string{"name.game", "abc"},
				ID:       263920338392,
			},
			wantErr: false,
		},
		{
			name: "invalid select query (without _ID)",
			fields: fields{
				input: `SELECT name.game, abc FROM users`,
			},
			want:    SelectOp{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Parser{
				input: tt.fields.input,
			}
			got, err := tr.ParseSelectQuery()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parser.ParseSelectQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parser.ParseSelectQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParser_ParseDeleteQuery(t *testing.T) {
	type fields struct {
		input string
	}
	tests := []struct {
		name    string
		fields  fields
		want    DeleteOp
		wantErr bool
	}{
		{
			name: "valid select query",
			fields: fields{
				input: `DELETE DOCUMENT FROM user WHERE _ID = 12`,
			},
			want:    DeleteOp{Document: "user", ID: 12},
			wantErr: false,
		},
		{
			name: "invalid select query (without _ID)",
			fields: fields{
				input: `DELETE DOCUMENT FROM user WHERE _ID = `,
			},
			want:    DeleteOp{},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Parser{
				input: tt.fields.input,
			}
			got, err := tr.ParseDeleteQuery()
			if (err != nil) != tt.wantErr {
				t.Errorf("Parser.ParseDeleteQuery() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Parser.ParseDeleteQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}

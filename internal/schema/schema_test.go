package schema

import (
	"strings"
	"testing"

	"github.com/nagarajRPoojari/orange/internal/query"
)

func TestSchemaHandler_Verifier(t *testing.T) {
	tests := []struct {
		name    string
		schema  query.Schema
		wantErr bool
		errSub  string
	}{
		{
			name: "Valid schema with _ID, name, age",
			schema: query.Schema(map[string]interface{}{
				"_ID":  map[string]interface{}{"auto_increment": false},
				"name": "DATETIME",
				"age":  map[string]interface{}{"name": "BYTE"},
			}),
			wantErr: false,
		},
		{
			name: "unsupported type COUNTRY",
			schema: query.Schema(map[string]interface{}{
				"name": "COUNTRY",
			}),
			wantErr: true,
			errSub:  "COUNTRY",
		},
		{
			name: "Only _ID is present",
			schema: query.Schema(map[string]interface{}{
				"_ID": map[string]interface{}{"auto_increment": true},
			}),
			wantErr: false,
		},
		{
			name: "Invalid _ID type",
			schema: query.Schema(map[string]interface{}{
				"_ID": "STRING", // should be map
			}),
			wantErr: true,
			errSub:  "_ID",
		},
		{
			name: "value provided instead of dtype",
			schema: query.Schema(map[string]interface{}{
				"active": 123, // invalid type, should be string or map
			}),
			wantErr: true,
			errSub:  "invalid",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := NewSchemaHandler(&SchemaHandlerOpts{}).VerifySchema(tc.schema)
			if (err != nil) != tc.wantErr {
				t.Fatalf("unexpected error status: got=%v, wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr && tc.errSub != "" && !strings.Contains(err.Error(), tc.errSub) {
				t.Errorf("expected error to contain '%s', got: %v", tc.errSub, err)
			}
		})
	}
}

func TestSchemaHandler_SavetoCatalog(t *testing.T) {

	type fields struct {
		opts *SchemaHandlerOpts
	}

	field := fields{opts: &SchemaHandlerOpts{Dir: t.TempDir()}}
	type args struct {
		docName string
		schema  query.Schema
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "valid schema and doc name",
			fields: field,
			args: args{docName: "test-0",
				schema: query.Schema(map[string]interface{}{
					"_ID":  map[string]interface{}{"auto_increment": false},
					"name": "DATETIME",
					"age":  map[string]interface{}{"name": "BYTE"},
				}),
			},
			wantErr: false,
		},
		{
			name:   "duplicate docName",
			fields: field,
			args: args{docName: "test-0",
				schema: query.Schema(map[string]interface{}{
					"_ID":  map[string]interface{}{"auto_increment": false},
					"name": "INT",
					"age":  map[string]interface{}{"name": "BYTE"},
				}),
			},
			wantErr: true,
		},
		{
			name:   "invalid schema",
			fields: field,
			args: args{docName: "test-1",
				schema: query.Schema(map[string]interface{}{
					"_ID": map[string]interface{}{
						"auto_increment": "int",
					}, // auto_increment should be bool
					"name": "FLOAT64",
					"age":  map[string]interface{}{"name": "BYTE"},
				}),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &SchemaHandler{
				opts: tt.fields.opts,
			}
			if err := tr.SavetoCatalog(tt.args.docName, tt.args.schema); (err != nil) != tt.wantErr {
				t.Errorf("SchemaHandler.SavetoCatalog() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSchemaHandler_VerifyAndCastData(t *testing.T) {
	type fields struct {
		opts *SchemaHandlerOpts
	}
	type args struct {
		schema map[string]interface{}
		data   map[string]interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "valid data",
			fields: fields{},
			args: args{
				schema: query.Schema(map[string]interface{}{
					"_ID":  map[string]interface{}{"auto_increment": false},
					"name": "STRING",
					"age":  map[string]interface{}{"name": "INT8"},
				}),
				data: map[string]interface{}{
					"_ID":  90102,
					"name": "hello",
					"age": map[string]interface{}{
						"name": 12,
					},
				},
			},
			wantErr: false,
		},
		{
			name:   "invalid decimal format",
			fields: fields{},
			args: args{
				schema: query.Schema(map[string]interface{}{
					"young": "DECIMAL",
					"old":   "BOOL",
					"name":  "INT",
				}),
				data: map[string]interface{}{
					"_ID":   0,
					"young": "16282929292.19191.1092",
					"old":   "false",
					"name":  819282,
				},
			},
			wantErr: true,
		},
		{
			name:   "valid with explicit type casting",
			fields: fields{},
			args: args{
				schema: query.Schema(map[string]interface{}{
					"young": "DECIMAL",
					"old":   "BOOL",
					"name":  "INT",
				}),
				data: map[string]interface{}{
					"_ID":   0,
					"young": "16282929292.192",
					"old":   "false",
					"name":  819282,
				},
			},
			wantErr: false,
		},
		{
			name:   "invalid to typecast",
			fields: fields{},
			args: args{
				schema: query.Schema(map[string]interface{}{
					"young": "INT",
					"old":   "BOOL",
				}),
				data: map[string]interface{}{
					"_ID":   0,
					"young": "false",
					"old":   "false",
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &SchemaHandler{
				opts: tt.fields.opts,
			}
			if err := tr.VerifyAndCastData(tt.args.schema, tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf(
					"SchemaHandler.VerifyAndCastData() error = %v, wantErr %v",
					err,
					tt.wantErr,
				)
			}
		})
	}
}

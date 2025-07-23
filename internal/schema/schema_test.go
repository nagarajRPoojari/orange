package schema

import (
	"strings"
	"testing"

	"github.com/nagarajRPoojari/orange/internal/query"
)

func TestSchemaVerifier(t *testing.T) {
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
				"age":  map[string]interface{}{"name": "BLOB"},
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

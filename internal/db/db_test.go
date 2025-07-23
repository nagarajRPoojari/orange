package db

import (
	"fmt"
	"testing"

	"github.com/nagarajRPoojari/orange/internal/query"
)

func TestOrangeDB_Create(t *testing.T) {
	db := NewOrangedb(
		DBopts{
			Dir: ".",
		},
	)

	err := db.CreateCollection(
		query.CreateOp{
			Document: "test",
			Schema: query.Schema(map[string]interface{}{
				"_ID":  map[string]interface{}{"auto_increment": false},
				"name": "STRING",
				"age":  map[string]interface{}{"name": "INT8"},
			}),
		},
	)

	fmt.Println(err)

	err = db.InsertDoc(
		query.InsertOp{
			Document: "test",
			Value: map[string]interface{}{
				"_ID":  90102,
				"name": "hello",
				"age": map[string]interface{}{
					"name": 12,
				},
			},
		},
	)

	fmt.Println(err)

	doc, err := db.GetDoc(
		query.SelectOp{
			Document: "test",
			ID:       90102,
		},
	)

	fmt.Println(doc, err)
}

func TestOragedb_CreateCollection(t *testing.T) {

	tempDir := t.TempDir()

	type fields struct {
		opts DBopts
	}
	type args struct {
		op query.CreateOp
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name:   "valid collection schema",
			fields: fields{opts: DBopts{Dir: tempDir}},
			args: args{
				op: query.CreateOp{
					Document: "test",
					Schema: query.Schema(map[string]interface{}{
						"_ID":  map[string]interface{}{"auto_increment": false},
						"name": "STRING",
						"age":  map[string]interface{}{"name": "INT8"},
					}),
				},
			},
			wantErr: false,
		},
		{
			name:   "invalid collection schema",
			fields: fields{opts: DBopts{Dir: tempDir}},
			args: args{
				op: query.CreateOp{
					Document: "test",
					Schema: query.Schema(map[string]interface{}{
						"_ID":  map[string]interface{}{"auto_increment": false},
						"name": "KK",
						"age":  map[string]interface{}{"name": "INT8"},
					}),
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := NewOrangedb(tt.fields.opts)
			if err := tr.CreateCollection(tt.args.op); (err != nil) != tt.wantErr {
				t.Errorf("Oragedb.CreateCollection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

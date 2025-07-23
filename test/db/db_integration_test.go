package db_test

import (
	"path"
	"testing"

	odb "github.com/nagarajRPoojari/orange/internal/db"
	"github.com/nagarajRPoojari/orange/internal/query"
	"github.com/nagarajRPoojari/orange/internal/types"
	"github.com/stretchr/testify/assert"
)

func TestOragedb_Init(t *testing.T) {
	dir := t.TempDir()
	db := odb.NewOrangedb(
		odb.DBopts{
			Dir: dir,
		},
	)

	assert.NotNil(t, db)

}

func TestOrangedb_SelectDoc(t *testing.T) {
	dir := t.TempDir()
	db := odb.NewOrangedb(
		odb.DBopts{
			Dir: dir,
		},
	)

	assert.NotNil(t, db)

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

	assert.FileExists(t, path.Join(dir, "catalog", "test"))
	assert.NoError(t, err, assert.AnError)
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

	assert.DirExists(t, path.Join(dir, "manifest"))
	assert.NoError(t, err, assert.AnError)

	got, err := db.GetDoc(
		query.SelectOp{
			Document: "test",
			ID:       90102,
		},
	)

	wanted := odb.InternalValueType(
		odb.InternalValueType{
			Payload: map[string]interface{}{"_ID": int64(90102), "age": map[string]interface{}{"name": types.INT8{V: 12}}, "name": types.STRING{V: "hello"}},
		})

	assert.Equal(t, wanted, got)
}

func TestOragedb_InsertDoc(t *testing.T) {
	dir := t.TempDir()
	db := odb.NewOrangedb(
		odb.DBopts{
			Dir: dir,
		},
	)

	assert.NotNil(t, db)

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

	assert.FileExists(t, path.Join(dir, "catalog", "test"))
	assert.NoError(t, err, assert.AnError)
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

	assert.DirExists(t, path.Join(dir, "manifest"))
	assert.NoError(t, err, assert.AnError)
}

func TestOragedb_CreateCollection(t *testing.T) {

	tempDir := t.TempDir()

	type fields struct {
		opts odb.DBopts
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
			fields: fields{opts: odb.DBopts{Dir: tempDir}},
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
			fields: fields{opts: odb.DBopts{Dir: tempDir}},
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
			tr := odb.NewOrangedb(tt.fields.opts)
			if err := tr.CreateCollection(tt.args.op); (err != nil) != tt.wantErr {
				t.Errorf("Oragedb.CreateCollection() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

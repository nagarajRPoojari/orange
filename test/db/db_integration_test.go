package db_test

import (
	"fmt"
	"path"
	"testing"

	"github.com/nagarajRPoojari/orange/parrot/utils/log"

	odb "github.com/nagarajRPoojari/orange/internal/db"
	"github.com/nagarajRPoojari/orange/internal/query"
	"github.com/nagarajRPoojari/orange/internal/types"
	"github.com/stretchr/testify/assert"
)

func init() {
	log.Disable()
}

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

	wanted := map[string]interface{}(
		map[string]interface{}{
			"_ID": types.ID{K: int64(90102)}, "age": map[string]interface{}{"name": types.INT8(12)}, "name": types.STRING("hello"),
		},
	)

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

func TestOrangedb_ProcessQuery(t *testing.T) {
	log.Disable()

	db := odb.NewOrangedb(odb.DBopts{Dir: t.TempDir()})
	_, err := db.ProcessQuery(`CREATE DOCUMENT users { "_ID": {"auto_increment": false},"name": "STRING", "age": {"value": "INT64"} }`)

	assert.NoError(t, err)

	for i := 1; i <= 100; i++ {
		name := fmt.Sprintf("User%d", i)
		age := i
		query := fmt.Sprintf(`INSERT VALUE INTO users {"_ID": %d, "name": "%s", "age": {"value": %d} }`, i, name, age)
		_, err := db.ProcessQuery(query)
		if err != nil {
			log.Fatalf("Insert failed for ID %d: %v", i, err)
		}
	}

	got, err := db.ProcessQuery(`SELECT name, age FROM users WHERE _ID = 89`)

	wanted := map[string]interface{}(
		map[string]interface{}{
			"_ID": types.ID{K: 89}, "age": map[string]interface{}{"value": types.INT64(89)}, "name": types.STRING("User89")},
	)
	assert.NoError(t, err)
	assert.Equal(t, wanted, got)
}

package db_test

import (
	"fmt"
	"path"
	"reflect"
	"testing"

	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	"github.com/spf13/viper"

	"github.com/nagarajRPoojari/orange/internal/config"
	odb "github.com/nagarajRPoojari/orange/internal/db"
	"github.com/nagarajRPoojari/orange/internal/types"
	"github.com/nagarajRPoojari/orange/pkg/query"
	"github.com/stretchr/testify/assert"
)

func init() {
	log.Disable()
}

func getMockedConfig(dir string) config.Config {
	viper.SetConfigName("mock")
	viper.SetConfigType("toml")
	viper.AddConfigPath("../")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Println(err)
		log.Fatalf("Config error: %v", err)
	}

	var cfg config.Config
	if err := viper.Unmarshal(&cfg); err != nil {
		fmt.Println(err)
		log.Fatalf("Unmarshal error: %v", err)
	}

	cfg.Directory = dir
	return cfg
}

func TestOragedb_Init(t *testing.T) {
	dir := t.TempDir()
	db := odb.NewOrangedb(
		t.Context(),
		getMockedConfig(dir),
	)
	assert.NotNil(t, db)
}

func TestOrangedb_SelectDoc(t *testing.T) {
	dir := t.TempDir()
	db := odb.NewOrangedb(
		t.Context(),
		getMockedConfig(dir),
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
	assert.NoError(t, err)
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

	assert.DirExists(t, path.Join(dir, "test", "manifest"))
	assert.NoError(t, err)

	got, err := db.GetDoc(
		query.SelectOp{
			Document: "test",
			ID:       90102,
		},
	)
	assert.NoError(t, err)

	wanted := map[string]interface{}(
		map[string]interface{}{
			"_ID": types.ID{
				K: int64(90102),
			}, "age": map[string]interface{}{"name": types.INT8(12)}, "name": types.STRING("hello"),
		},
	)

	assert.Equal(t, wanted, got)
}

func TestOrangedb_DeleteDoc(t *testing.T) {
	dir := t.TempDir()
	db := odb.NewOrangedb(
		t.Context(),
		getMockedConfig(dir),
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
	assert.NoError(t, err)
	err = db.InsertDoc(
		query.InsertOp{
			Document: "test",
			Value: map[string]interface{}{
				"_ID":  int64(90102),
				"name": "hello",
				"age": map[string]interface{}{
					"name": 12,
				},
			},
		},
	)

	assert.DirExists(t, path.Join(dir, "test", "manifest"))
	assert.NoError(t, err)

	err = db.DeleteDoc(
		query.DeleteOp{
			Document: "test",
			ID:       int64(90102),
		},
	)

	assert.NoError(t, err)
	_, err = db.GetDoc(
		query.SelectOp{
			Document: "test",
			ID:       int64(90102),
		},
	)
	assert.Error(t, err)
}

func TestOragedb_InsertDoc(t *testing.T) {
	dir := t.TempDir()
	db := odb.NewOrangedb(
		t.Context(),
		getMockedConfig(dir),
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
	assert.NoError(t, err)
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

	assert.DirExists(t, path.Join(dir, "test", "manifest"))
	assert.NoError(t, err)
}

func TestOragedb_CreateCollection(t *testing.T) {

	tempDir := t.TempDir()

	type fields struct {
		conf config.Config
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
			fields: fields{conf: getMockedConfig(tempDir)},
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
			fields: fields{conf: getMockedConfig(tempDir)},
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
			tr := odb.NewOrangedb(t.Context(), tt.fields.conf)
			err := tr.CreateCollection(tt.args.op)
			assert.Equal(t, tt.wantErr, err != nil, "Oragedb.CreateCollection() error = %v", err)
		})
	}
}

func TestOrangedb_ProcessQuery(t *testing.T) {
	log.Disable()

	db := odb.NewOrangedb(t.Context(), getMockedConfig(t.TempDir()))
	_, err := db.ProcessQuery(
		`CREATE DOCUMENT users { "_ID": {"auto_increment": false},"name": "STRING", "age": {"value": "INT64"} }`,
	)
	assert.NoError(t, err)

	for i := 1; i <= 100; i++ {
		name := fmt.Sprintf("User%d", i)
		age := i
		query := fmt.Sprintf(
			`INSERT VALUE INTO users {"_ID": %d, "name": "%s", "age": {"value": %d} }`,
			i,
			name,
			age,
		)
		_, err := db.ProcessQuery(query)
		assert.NoError(t, err, "Insert failed for ID %d: %v", i, err)
	}

	got, err := db.ProcessQuery(`SELECT name, age FROM users WHERE _ID = 89`)
	assert.NoError(t, err)
	wantedA := map[string]interface{}(
		map[string]interface{}{
			"_ID": types.ID{
				K: 89,
			}, "age": map[string]interface{}{"value": types.INT64(89)}, "name": "User89"},
	)
	wantedB := map[string]interface{}(
		map[string]interface{}{
			"_ID": types.ID{
				K: 89,
			}, "age": map[string]interface{}{"value": types.INT64(89)}, "name": types.STRING("User89")},
	)
	assert.True(t,
		reflect.DeepEqual(got, wantedA) || reflect.DeepEqual(got, wantedB),
		"Expected result to match either wantedA or wantedB, but got: %v", got,
	)
	_, err = db.ProcessQuery(`DELETE DOCUMENT FROM users WHERE _ID = 89`)
	assert.NoError(t, err)

	_, err = db.ProcessQuery(`SELECT name, age FROM users WHERE _ID = 89`)
	assert.Error(t, err)
}

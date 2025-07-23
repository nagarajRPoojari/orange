package schema_test

import (
	"testing"

	"github.com/nagarajRPoojari/orange/internal/query"
	"github.com/nagarajRPoojari/orange/internal/schema"
	"github.com/stretchr/testify/assert"
)

func TestSchemaHandler_LoadFromCatalog(t *testing.T) {
	dir := t.TempDir()

	docName, wanted := "user", query.Schema(map[string]interface{}{
		"_ID":  map[string]interface{}{"auto_increment": false},
		"name": "STRING",
		"age":  map[string]interface{}{"name": "INT8"},
	})

	sh := schema.NewSchemaHandler(&schema.SchemaHandlerOpts{Dir: dir})
	err := sh.SavetoCatalog(docName, wanted)
	assert.NoError(t, err, assert.AnError)

	got, err := sh.LoadFromCatalog(docName)
	assert.NoError(t, err, assert.AnError)

	assert.Equal(t, wanted, got)
}

package client

import (
	"os"
	"testing"
	"time"

	"github.com/nagarajRPoojari/orange/net/client"
	"github.com/nagarajRPoojari/orange/net/server"
	"github.com/nagarajRPoojari/orange/pkg/query"
	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	dbServer := server.NewServer()
	go dbServer.Run()

	time.Sleep(2 * time.Second)
	cl := client.NewClient()

	err := cl.Create(
		&query.CreateOp{
			Document: "test",
			Schema: query.Schema(map[string]interface{}{
				"_ID":  map[string]interface{}{"auto_increment": false},
				"name": "STRING",
				"age":  map[string]interface{}{"name": "INT8"},
			}),
		},
	)
	assert.NoError(t, err)

	err = cl.Insert(
		&query.InsertOp{
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
	assert.NoError(t, err)

	got, err := cl.Select(
		&query.SelectOp{
			Document: "test",
			ID:       90102,
		},
	)
	assert.NoError(t, err)
	assert.NotNil(t, got)

	os.RemoveAll("./temp")
	dbServer.Stop()
}

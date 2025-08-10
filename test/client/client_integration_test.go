package client

import (
	"os"
	"testing"
	"time"

	"github.com/nagarajRPoojari/orange/net/client"
	"github.com/nagarajRPoojari/orange/net/server"
	"github.com/nagarajRPoojari/orange/pkg/oql"
	"github.com/stretchr/testify/assert"
)

func TestClient(t *testing.T) {
	dbServer := server.NewServer("localhost", 52001, &server.ReplicationOpts{})
	go dbServer.Run()

	time.Sleep(2 * time.Second)
	cl := client.NewClient("localhost", 52001)

	err := cl.Create(
		&oql.CreateOp{
			Document: "test",
			Schema: oql.Schema(map[string]interface{}{
				"_ID":  map[string]interface{}{"auto_increment": false},
				"name": "STRING",
				"age":  map[string]interface{}{"name": "INT8"},
			}),
		},
	)
	assert.NoError(t, err)

	err = cl.Insert(
		&oql.InsertOp{
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
		&oql.SelectOp{
			Document: "test",
			ID:       90102,
		},
	)
	assert.NoError(t, err)
	assert.NotNil(t, got)

	os.RemoveAll("./temp")
	dbServer.Stop()
}

package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nagarajRPoojari/orange/net/client"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	"github.com/nagarajRPoojari/orange/pkg/oql"
	"github.com/stretchr/testify/assert"
)

const baseURL = "http://localhost:8000/"

func processoql(client *client.Client, q string) (any, error) {
	parser := oql.NewParser(q)
	op, err := parser.Build()
	if err != nil {
		return nil, err
	}

	switch v := op.(type) {
	case oql.CreateOp:
		return nil, client.Create(&v)
	case oql.InsertOp:
		return nil, client.Insert(&v)
	case oql.SelectOp:
		return client.Select(&v)
	case oql.DeleteOp:
		return nil, fmt.Errorf("delete op not implpemented")
	}

	return nil, fmt.Errorf("syntax error: invalid op")
}

func BenchmarkIO(b *testing.B) {
	cl := client.NewClient("localhost", 8000)
	log.Disable()

	doc := uuid.NewString()
	createSchemaQuery := fmt.Sprintf(`create document %s {"name":"STRING"}`, doc)
	_, err := processoql(cl, createSchemaQuery)
	assert.NoError(b, err)

	b.ResetTimer()
	ops := 10000
	start := time.Now()

	ticket := make(chan struct{}, 1000)
	wg := sync.WaitGroup{}
	for i := 1; i <= ops; i++ {
		wg.Add(1)
		ticket <- struct{}{} // acquire ticket
		go func() {
			defer func() {
				<-ticket // release ticket
				wg.Done()
			}()
			insertQuery := fmt.Sprintf(`insert value into %s {"_ID": %d, "name": "hello-%d"}`, doc, i, i)
			_, err = processoql(cl, insertQuery)
			assert.NoError(b, err)
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	opsPerSec := float64(ops) / elapsed
	fmt.Printf("Total time taken: %v, Ops/sec: %.2fM\n", elapsed, opsPerSec/MILLION)
}

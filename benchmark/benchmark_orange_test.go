package storage

import (
	"fmt"
	"math/rand"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nagarajRPoojari/orange/net/client"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	"github.com/nagarajRPoojari/orange/pkg/oql"
	"github.com/stretchr/testify/assert"
)

const baseURL = "http://localhost:8000/"

// BenchmarkOrange_Write measures the performance of concurrent write queries
// against a document store. It executes parallel INSERT queries with random
// id each with small payload, nearly <64 bytes
func BenchmarkOrange_Write(b *testing.B) {
	if !isPortOpen("localhost", 8000, time.Second) {
		b.Logf("failed to establish connection with orangedb server")
		return
	}

	cl := client.NewClient("localhost", 8000)
	log.Disable()

	bigChunk := "value-string"
	payloadSize := len(bigChunk)

	doc := uuid.NewString()
	createSchemaQuery := fmt.Sprintf(`create document %s {"name":"STRING"}`, doc)
	_, err := processoql(cl, createSchemaQuery)
	assert.NoError(b, err)

	start := time.Now()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := int(time.Now().UnixNano())
			insertQuery := fmt.Sprintf(`insert value into %s {"_ID": %d, "name": "%s"}`, doc, id, bigChunk)
			_, err := processoql(cl, insertQuery)
			assert.NoError(b, err)
		}
	})

	b.StopTimer()
	elapsed := time.Since(start)

	totalBytes := float64(b.N * payloadSize)
	mbWritten := totalBytes / (1024 * 1024)
	opsPerSec := float64(b.N) / elapsed.Seconds()
	mbPerSec := mbWritten / elapsed.Seconds()
	avgLatencyNs := float64(elapsed.Nanoseconds()) / float64(b.N)
	avgLatencyMs := avgLatencyNs / 1_000

	BenchmarkReport{
		Name:                      "orange/write",
		TotalOps:                  b.N,
		PayloadSizeInBytes:        payloadSize,
		TotalDataTransferredInMB:  totalBytes,
		TotalTimeTakenInSec:       elapsed.Seconds(),
		OpsPerSec:                 opsPerSec,
		DataTransferredInMBPerSec: mbPerSec,
		AverageLatencyInMicroSec:  avgLatencyMs,
	}.Dump("benchmark-orange-write.json")
}

// BenchmarkOrange_Write_With_JumboPayload similar to BenchmarkWrite measures performance
// of write queries but with huge payloads of size ~20kb. Specifically it tests
// bytes throughput under huge payloads.
func BenchmarkOrange_Write_With_JumboPayload(b *testing.B) {
	if !isPortOpen("localhost", 8000, time.Second) {
		b.Logf("failed to establish connection with orangedb server")
		return
	}

	cl := client.NewClient("localhost", 8000)
	log.Disable()

	// using a 20kb+ payload per write
	bigChunk := jumboPayload(10 * 1024)
	payloadSize := len(bigChunk)

	doc := uuid.NewString()
	createSchemaQuery := fmt.Sprintf(`create document %s {"name":"STRING"}`, doc)
	_, err := processoql(cl, createSchemaQuery)
	assert.NoError(b, err)

	start := time.Now()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := int(time.Now().UnixNano())
			insertQuery := fmt.Sprintf(`insert value into %s {"_ID": %d, "name": "%s"}`, doc, id, bigChunk)
			_, err := processoql(cl, insertQuery)
			assert.NoError(b, err)
		}
	})

	b.StopTimer()
	elapsed := time.Since(start)

	totalBytes := float64(b.N * payloadSize)
	mbWritten := totalBytes / (1024 * 1024)
	mbPerSec := mbWritten / elapsed.Seconds()
	opsPerSec := float64(b.N) / elapsed.Seconds()
	avgLatencyNs := float64(elapsed.Nanoseconds()) / float64(b.N)
	avgLatencyMs := avgLatencyNs / 1_000

	BenchmarkReport{
		Name:                      "orange/write with jumbo payload",
		TotalOps:                  b.N,
		PayloadSizeInBytes:        payloadSize,
		TotalDataTransferredInMB:  totalBytes,
		TotalTimeTakenInSec:       elapsed.Seconds(),
		OpsPerSec:                 opsPerSec,
		DataTransferredInMBPerSec: mbPerSec,
		AverageLatencyInMicroSec:  avgLatencyMs,
	}.Dump("benchmark-orange-write-with-jumbo-payload.json")
}

// BenchmarkOrange_Read measures the performance of concurrent read queries
// against a document store. It first inserts b.N records, then
// executes parallel SELECT queries on random record IDs to
// evaluate throughput and latency.
func BenchmarkOrange_Read(b *testing.B) {
	cl := client.NewClient("localhost", 8000)
	log.Disable()

	bigChunk := "value-string"
	payloadSize := len(bigChunk)

	doc := uuid.NewString()
	createSchemaQuery := fmt.Sprintf(`create document %s {"name":"STRING"}`, doc)
	_, err := processoql(cl, createSchemaQuery)
	assert.NoError(b, err)

	for i := 0; i < b.N; i++ {
		insertQuery := fmt.Sprintf(`insert value into %s {"_ID": %d, "name": "%s"}`, doc, i, bigChunk)
		_, err := processoql(cl, insertQuery)
		assert.NoError(b, err)
	}

	start := time.Now()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := RandomKey(0, b.N)
			readQuery := fmt.Sprintf(`select * from %s where _ID = %d`, doc, id)
			_, err := processoql(cl, readQuery)
			assert.NoError(b, err)
		}
	})

	b.StopTimer()
	elapsed := time.Since(start)

	totalBytes := float64(b.N * payloadSize)
	mbRead := totalBytes / (1024 * 1024)
	opsPerSec := float64(b.N) / elapsed.Seconds()
	mbPerSec := mbRead / elapsed.Seconds()
	avgLatencyNs := float64(elapsed.Nanoseconds()) / float64(b.N)
	avgLatencyMs := avgLatencyNs / 1_000

	BenchmarkReport{
		Name:                      "orange/read",
		TotalOps:                  b.N,
		PayloadSizeInBytes:        payloadSize,
		TotalDataTransferredInMB:  totalBytes,
		TotalTimeTakenInSec:       elapsed.Seconds(),
		OpsPerSec:                 opsPerSec,
		DataTransferredInMBPerSec: mbPerSec,
		AverageLatencyInMicroSec:  avgLatencyMs,
	}.Dump("benchmark-orange-read.json")
}

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

func jumboPayload(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	var sb strings.Builder
	sb.Grow(n)

	for i := 0; i < n; i++ {
		sb.WriteByte(letters[rand.Intn(len(letters))])
	}

	return sb.String()
}

func isPortOpen(host string, port int, timeout time.Duration) bool {
	address := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

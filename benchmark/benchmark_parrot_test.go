// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package storage

import (
	"context"
	"os"
	"runtime/pprof"
	"sync/atomic"
	"testing"
	"time"

	storage "github.com/nagarajRPoojari/orange/parrot"
	"github.com/nagarajRPoojari/orange/parrot/conf"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"

	"github.com/nagarajRPoojari/orange/parrot/memtable"
	"github.com/nagarajRPoojari/orange/parrot/metadata"
	"github.com/nagarajRPoojari/orange/parrot/types"
)

const MILLION = 10_00_000

// BenchmarkParrot_Read benchmarks concurrent reads
// after flushing a large number of entries to disk-backed memtables.
//
//   - sst/memtable size is set to 2MB
//   - WAL is disabled
//   - cache & manifest sync() are enabled
func BenchmarkParrot_Read(b *testing.B) {
	log.Disable()

	dbName := "test"
	tempDir := b.TempDir()

	const MEMTABLE_THRESHOLD = 1024 * 4 * 1024
	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	db := storage.NewStorage[types.IntKey, *types.IntValue](
		dbName,
		ctx,
		storage.StorageOpts{
			Directory:                     tempDir,
			MemtableThreshold:             MEMTABLE_THRESHOLD,
			TurnOnMemtableWal:             true,
			FlushTimeInterval:             conf.DefaultFlusherTimeInterval,
			MemtableWALTimeInterval:       conf.DefaultWALTimeInterval,
			MemtableWALEventChSize:        conf.DefaultWALEventBufferSize,
			MemtableWALWriterBufferSize:   conf.DefaultWALEventBufferSize,
			TurnOnCompaction:              true,
			CompactionTimeInterval:        conf.DefaultCompactionTimeInterval,
			CompactionWALTimeInterval:     conf.DefaultWALTimeInterval,
			CompactionWALEventChSize:      conf.DefaultWALEventBufferSize,
			CompactionWALWriterBufferSize: conf.DefaultWriterBufferSize,
			Level0MaxSizeInBytes:          1024 * 2,
			MaxSizeInBytesGrowthFactor:    2,
		},
	)
	for i := range b.N {
		db.Put(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}

	time.Sleep(1 * time.Second)
	var totalLatencyNs int64

	start := time.Now()
	b.ResetTimer()
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			i := RandomKey(0, b.N)
			start := time.Now()
			readStatus := db.Get(types.IntKey{K: i})
			v := types.IntValue{V: int32(i)}
			elapsed := time.Since(start).Nanoseconds()
			atomic.AddInt64(&totalLatencyNs, elapsed)
			if readStatus.Err != nil || *readStatus.Value != v {
				b.Errorf("Expected %v, got %v", v, readStatus)
			}
		}
	})

	payloadSize := 16
	elapsed := time.Since(start)
	opsPerSec := float64(b.N) / elapsed.Seconds()
	avgLatencyNs := float64(totalLatencyNs) / float64(b.N)
	avgLatencyMs := avgLatencyNs / 1_000_000

	BenchmarkReport{
		TotalOps:                   b.N,
		PayloadSize:                payloadSize, // 16 bytes
		TotalBytesTransferred:      float64(b.N * payloadSize),
		TotalTimeTaken:             elapsed.Seconds(),
		OpsPerSec:                  opsPerSec,
		MegaBytesTransferredPerSec: float64(b.N*payloadSize) / elapsed.Seconds(),
		AverageLatency:             avgLatencyMs,
	}.Dump("benchmark-parrot-read.json")
	dumpGoroutines()
}

// BenchmarkParrot_Write_With_WAL benchmarks serial writes
// with WAL turned on
//
//   - sst/memtable size is set to 2MB
//   - WAL is enabled
//   - cache & manifest sync() are enabled
func BenchmarkParrot_Write_With_WAL(b *testing.B) {
	log.Disable()

	const MEMTABLE_THRESHOLD = 1024 * 2 * 1024
	temp := b.TempDir()
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: temp})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	b.Cleanup(cancel)

	mf.SyncLoop(ctx)

	// overflow first memtable to trigger flush
	mts := memtable.NewMemtableStore[types.IntKey, *types.IntValue](
		mf,
		ctx,
		memtable.MemtableOpts{
			MemtableSoftLimit:   MEMTABLE_THRESHOLD,
			TurnOnWal:           true,
			WALLogDir:           temp,
			WALTimeInterval:     conf.DefaultWALTimeInterval,
			WALEventChSize:      conf.DefaultWALEventBufferSize,
			WALWriterBufferSize: conf.DefaultWALEventBufferSize,
			FlushTimeInterval:   1000 * time.Millisecond,
		})

	var totalLatencyNs int64
	start := time.Now()
	b.ResetTimer()
	for i := range b.N {
		mts.Write(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}
	elapsed := time.Since(start)
	payloadSize := 16
	opsPerSec := float64(b.N) / elapsed.Seconds()
	avgLatencyNs := float64(totalLatencyNs) / float64(b.N)
	avgLatencyMs := avgLatencyNs / 1_000_000

	BenchmarkReport{
		TotalOps:                   b.N,
		PayloadSize:                payloadSize, // 16 bytes
		TotalBytesTransferred:      float64(b.N * payloadSize),
		TotalTimeTaken:             elapsed.Seconds(),
		OpsPerSec:                  opsPerSec,
		MegaBytesTransferredPerSec: float64(b.N*payloadSize) / elapsed.Seconds(),
		AverageLatency:             avgLatencyMs,
	}.Dump("benchmark-parrot-write-with-wal.json")
	dumpGoroutines()
}

// BenchmarkParrot_Write_Without_WAL benchmarks serial writes
// with WAL turned off
//
//   - sst/memtable size is set to 2MB
//   - WAL is disabled
//   - cache & manifest sync() are enabled
func BenchmarkParrot_Write_Without_WAL(b *testing.B) {
	log.Disable()
	ctx := b.Context()

	const MEMTABLE_THRESHOLD = 1024 * 2 * 1024
	temp := b.TempDir()
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: temp})
	mf.Load()

	mf.SyncLoop(ctx)

	mts := memtable.NewMemtableStore[types.IntKey, *types.IntValue](
		mf,
		ctx,
		memtable.MemtableOpts{
			MemtableSoftLimit: MEMTABLE_THRESHOLD,
			TurnOnWal:         false,
			FlushTimeInterval: 1000 * time.Millisecond,
		})

	var totalLatencyNs int64
	start := time.Now()
	b.ResetTimer()
	for i := range b.N {
		mts.Write(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}
	elapsed := time.Since(start)
	payloadSize := 16
	opsPerSec := float64(b.N) / elapsed.Seconds()
	avgLatencyNs := float64(totalLatencyNs) / float64(b.N)
	avgLatencyMs := avgLatencyNs / 1_000_000

	BenchmarkReport{
		TotalOps:                   b.N,
		PayloadSize:                payloadSize, // 16 bytes
		TotalBytesTransferred:      float64(b.N * payloadSize),
		TotalTimeTaken:             elapsed.Seconds(),
		OpsPerSec:                  opsPerSec,
		MegaBytesTransferredPerSec: float64(b.N*payloadSize) / elapsed.Seconds(),
		AverageLatency:             avgLatencyMs,
	}.Dump("benchmark-parrot-write-without-wal.json")
	dumpGoroutines()
}

func dumpGoroutines() {
	f, err := os.Create("../goroutine.prof")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	pprof.Lookup("goroutine").WriteTo(f, 1)
}

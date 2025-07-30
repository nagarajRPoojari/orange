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
	"sync"
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

// BenchmarkMemtable_Read benchmarks concurrent reads
// after flushing a large number of entries to disk-backed memtables.
//
//   - sst/memtable size is set to 2kb
//   - WAL is disabled
//   - cache & manifest sync() are enabled
//   - limits concurrent read threads to 5000 to prevent lock starvation
func BenchmarkMemtable_Read(t *testing.B) {
	log.Disable()

	dbName := "test"

	tempDir := "."

	const MEMTABLE_THRESHOLD = 1024 * 4 * 1024
	const MAX_CONCURRENT_READ_ROUTINES = 500
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	db := storage.NewStorage[types.IntKey, *types.IntValue](
		dbName,
		ctx,
		storage.StorageOpts{
			Directory:                     tempDir,
			MemtableThreshold:             MEMTABLE_THRESHOLD,
			TurnOnMemtableWal:             true,
			FlushTimeInterval:             1000 * time.Millisecond,
			MemtableWALTimeInterval:       conf.DefaultWALTimeInterval,
			MemtableWALEventChSize:        conf.DefaultWALEventBufferSize,
			MemtableWALWriterBufferSize:   conf.DefaultWALEventBufferSize,
			TurnOnCompaction:              true,
			CompactionTimeInterval:        1000 * time.Millisecond,
			CompactionWALTimeInterval:     conf.DefaultWALTimeInterval,
			CompactionWALEventChSize:      conf.DefaultWALEventBufferSize,
			CompactionWALWriterBufferSize: conf.DefaultWriterBufferSize,
			Level0MaxSizeInBytes:          1024 * 2,
			MaxSizeInBytesGrowthFactor:    2,
		},
	)

	d := types.IntValue{V: 0}

	multiples := 10
	totalOps := int(MEMTABLE_THRESHOLD/d.SizeOf()) * multiples

	for i := range totalOps {
		db.Put(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}

	time.Sleep(1 * time.Second)
	wg := sync.WaitGroup{}

	start := time.Now()
	ticket := make(chan struct{}, MAX_CONCURRENT_READ_ROUTINES)
	for i := 0; i < totalOps; i++ {
		wg.Add(1)

		ticket <- struct{}{} // acquire ticket
		go func(i int) {
			defer func() {
				<-ticket // release ticket
				wg.Done()
			}()

			readStatus := db.Get(types.IntKey{K: i})
			v := types.IntValue{V: int32(i)}
			if readStatus.Err != nil || *readStatus.Value != v {
				t.Errorf("Expected %v, got %v", v, readStatus)
			}
		}(i)
	}

	wg.Wait()
	t.Logf("total ops = %d", totalOps)
	elapsed := time.Since(start)

	opsPerSec := float64(totalOps) / elapsed.Seconds()
	t.Logf("Total time taken: %v, Ops/sec: %.2fM", elapsed, opsPerSec/MILLION)

	dumpGoroutines()
}

// BenchmarkMemtable_Write_With_WAL benchmarks serial writes
// with WAL turned on
//
//   - sst/memtable size is set to 2MB
//   - WAL is enabled
//   - cache & manifest sync() are enabled
func BenchmarkMemtable_Write_With_WAL(t *testing.B) {
	log.Disable()

	const MEMTABLE_THRESHOLD = 1024 * 2 * 1024
	temp := "test"
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: temp})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

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
	d := types.IntValue{V: 0}

	multiples := 10
	totalOps := int(MEMTABLE_THRESHOLD/d.SizeOf()) * multiples

	start := time.Now()

	for i := range totalOps {
		mts.Write(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}

	t.Logf("total ops = %d", totalOps)
	elapsed := time.Since(start)
	opsPerSec := float64(totalOps) / elapsed.Seconds()
	t.Logf("Total time taken: %v, Ops/sec: %.2fM", elapsed, opsPerSec/MILLION)

	// cleanDirectory(temp)
	dumpGoroutines()
}

// BenchmarkMemtable_Write_Without_WAL benchmarks serial writes
// with WAL turned off
//
//   - sst/memtable size is set to 2MB
//   - WAL is disabled
//   - cache & manifest sync() are enabled
func BenchmarkMemtable_Write_Without_WAL(t *testing.B) {
	log.Disable()
	ctx := t.Context()

	const MEMTABLE_THRESHOLD = 1024 * 2 * 1024
	temp := "test"
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: temp})
	mf.Load()

	mf.SyncLoop(ctx)

	// overflow first memtable to trigger flush
	mts := memtable.NewMemtableStore[types.IntKey, *types.IntValue](
		mf,
		ctx,
		memtable.MemtableOpts{
			MemtableSoftLimit: MEMTABLE_THRESHOLD,
			TurnOnWal:         false,
			FlushTimeInterval: 1000 * time.Millisecond,
		})
	d := types.IntValue{V: 0}

	multiples := 10
	totalOps := int(MEMTABLE_THRESHOLD/d.SizeOf()) * multiples

	start := time.Now()

	for i := range totalOps {
		mts.Write(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}

	t.Logf("total ops = %d", totalOps)
	elapsed := time.Since(start)
	opsPerSec := float64(totalOps) / elapsed.Seconds()
	t.Logf("Total time taken: %v, Ops/sec: %.2fM", elapsed, opsPerSec/MILLION)

	// cleanDirectory(temp)
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

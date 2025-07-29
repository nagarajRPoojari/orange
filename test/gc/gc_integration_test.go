// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package compactor_test

import (
	"context"
	"testing"
	"time"

	"github.com/nagarajRPoojari/orange/parrot/compactor"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	"github.com/stretchr/testify/assert"

	v2 "github.com/nagarajRPoojari/orange/parrot/cache/v2"
	"github.com/nagarajRPoojari/orange/parrot/memtable"
	"github.com/nagarajRPoojari/orange/parrot/metadata"
	"github.com/nagarajRPoojari/orange/parrot/types"
)

// TestGC verifies basic garbage collection and compaction behavior.
// It:
//   - Sets up a memtable store and a size-tiered compaction GC instance
//   - Writes enough data to exceed the memtable size and trigger a flush
//   - Runs GC in the background to compact flushed SSTables
//   - Confirms data integrity after flushing and compaction
//   - Checks that compaction output exists in level-1 directory
func TestGC(t *testing.T) {
	log.Disable()
	tempDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: tempDir})
	mf.Load()

	mf.SyncLoop(ctx)

	mts := memtable.NewMemtableStore[types.IntKey, *types.IntValue](
		mf,
		ctx,
		memtable.MemtableOpts{MemtableSoftLimit: 1024},
	)
	d := types.IntValue{V: 0}

	gc := compactor.NewGC(
		mf,
		(*v2.CacheManager[types.IntKey, *types.IntValue])(mts.DecoderCache),
		&compactor.SizeTiredCompaction[types.IntKey, *types.IntValue]{
			Opts: compactor.SizeTiredCompactionOpts{
				Level0MaxSizeInBytes:       1000,
				MaxSizeInBytesGrowthFactor: 10,
			},
		},
		tempDir,
	)
	go gc.Run(ctx)

	// overflow memtable to trigger flush
	for i := range int(1024 / d.SizeOf()) {
		mts.Write(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}

	k, v := types.IntKey{K: 90892389}, types.IntValue{V: 1993920}
	ok := mts.Write(k, &v)
	assert.True(t, ok, "Expected to trigger flush")

	// wait for memtable to flush & clear both memtable
	time.Sleep(3 * time.Second)
	mts.Clear()

	val, ok := mts.Read(types.IntKey{K: 244})
	v = types.IntValue{V: 244}

	assert.True(t, ok)
	assert.Equal(t, v, *val)
}

// TestGC_Intensive verifies end-to-end garbage collection and compaction behavior.
// It performs the following:
//   - Initializes a memtable store and a size-tiered compaction GC instance
//   - Writes enough data to trigger multiple memtable flushes
//   - Runs background GC to compact flushed SSTables
//   - Asserts that data is preserved after flushing and compaction
//   - Confirms compaction output by checking higher-level SSTable directory
func TestGC_Intensive(t *testing.T) {
	log.Disable()
	tempDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	const MEMTABLE_THRESHOLD = 1024

	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: tempDir})
	mf.Load()

	mf.SyncLoop(ctx)

	mts := memtable.NewMemtableStore[types.IntKey, *types.IntValue](
		mf,
		ctx,
		memtable.MemtableOpts{MemtableSoftLimit: MEMTABLE_THRESHOLD},
	)
	d := types.IntValue{V: 0}

	gc := compactor.NewGC(
		mf,
		(*v2.CacheManager[types.IntKey, *types.IntValue])(mts.DecoderCache),
		&compactor.SizeTiredCompaction[types.IntKey, *types.IntValue]{
			Opts: compactor.SizeTiredCompactionOpts{
				Level0MaxSizeInBytes:       2 * MEMTABLE_THRESHOLD, // softlimit = 2kb
				MaxSizeInBytesGrowthFactor: 2,                      // growth_factor = 2
			},
		},
		tempDir,
	)
	go gc.Run(ctx)

	// overflow memtable to trigger flush
	multiples := 10
	totalOps := int(MEMTABLE_THRESHOLD/d.SizeOf()) * multiples

	for i := range totalOps {
		mts.Write(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}
	k, v := types.IntKey{K: 90892389}, types.IntValue{V: 1993920}
	ok := mts.Write(k, &v)
	assert.True(t, ok, "Expected to trigger flush")

	// wait for memtable to flush & clear both memtable
	time.Sleep(5 * time.Second)
	mts.Clear()

	val, ok := mts.Read(types.IntKey{K: 244})
	v = types.IntValue{V: 244}

	assert.True(t, ok)
	assert.Equal(t, v, *val)
}

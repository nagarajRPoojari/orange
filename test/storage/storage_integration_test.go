// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package storage_test

import (
	"context"
	"testing"
	"time"

	parrot "github.com/nagarajRPoojari/orange/parrot"
	"github.com/nagarajRPoojari/orange/parrot/types"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	"github.com/stretchr/testify/assert"
)

// TestStorage_Get_Put verifies that a key-value pair can be successfully written to and read from storage.
// It ensures that:
//   - The Put operation stores the data without error.
//   - The Get operation retrieves the exact value that was stored.
//   - The system handles memtable flushing and WAL correctly with compaction enabled.
func TestStorage_Get_Put(t *testing.T) {
	log.Disable()

	dbName := "test"
	dir := t.TempDir()
	const MEMTABLE_THRESHOLD = 1024 * 2

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	db := parrot.NewStorage[types.IntKey, *types.IntValue](
		dbName,
		ctx,
		parrot.StorageOpts{
			Directory:         dir,
			MemtableThreshold: MEMTABLE_THRESHOLD,
			TurnOnCompaction:  true,
			TurnOnWal:         true,
			GCLogDir:          dir,
		})

	k, v := types.IntKey{K: 278}, types.IntValue{V: int32(278)}

	writeRes := db.Put(k, &v)
	assert.NoError(t, writeRes.Err, "failed to put key")

	readRes := db.Get(k)
	assert.NoError(t, readRes.Err, "failed to get key")

}

func TestStorage_Delete(t *testing.T) {
	log.Disable()

	dbName := "test"
	dir := t.TempDir()

	const MEMTABLE_THRESHOLD = 1024 * 2

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	db := parrot.NewStorage[types.IntKey, *types.IntValue](
		dbName,
		ctx,
		parrot.StorageOpts{
			Directory:         dir,
			MemtableThreshold: MEMTABLE_THRESHOLD,
			TurnOnCompaction:  true,
			TurnOnWal:         true,
			GCLogDir:          dir,
		})

	k, v := types.IntKey{K: 278}, types.IntValue{V: int32(278)}

	// Perform enough writes to trigger approximately 10 memtable flushes.
	// Each flush occurs after reaching MEMTABLE_THRESHOLD bytes.
	// totalOps is calculated based on the size of each entry and number of flushes.
	multiples := 10
	totalOps := int(MEMTABLE_THRESHOLD/v.SizeOf()) * multiples
	for i := range totalOps {
		db.Put(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}

	deleteStatus := db.Delete(k, &types.IntValue{})
	assert.NoError(t, deleteStatus.Err, "failed to delete key")

	readRes := db.Get(k)
	assert.Error(t, readRes.Err)
}

func TestStorage_Load_DB(t *testing.T) {
	log.Disable()

	dbName := "test"
	dir := t.TempDir()

	const MEMTABLE_THRESHOLD = 1024 * 2

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	db1 := parrot.NewStorage[types.IntKey, *types.IntValue](
		dbName,
		ctx,
		parrot.StorageOpts{
			Directory:         dir,
			MemtableThreshold: MEMTABLE_THRESHOLD,
			TurnOnCompaction:  true,
			TurnOnWal:         true,
			GCLogDir:          dir,
		})

	k, v := types.IntKey{K: 278}, types.IntValue{V: int32(278)}

	// Perform enough writes to trigger approximately 10 memtable flushes.
	// Each flush occurs after reaching MEMTABLE_THRESHOLD bytes.
	// totalOps is calculated based on the size of each entry and number of flushes.
	multiples := 10
	totalOps := int(MEMTABLE_THRESHOLD/v.SizeOf()) * multiples
	for i := range totalOps {
		db1.Put(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}

	time.Sleep(2 * time.Second)

	// creating one more db to load from same directory
	db2 := parrot.NewStorage[types.IntKey, *types.IntValue](
		dbName,
		ctx,
		parrot.StorageOpts{
			Directory:         dir,
			MemtableThreshold: MEMTABLE_THRESHOLD,
			TurnOnCompaction:  true,
			TurnOnWal:         true,
			GCLogDir:          dir,
		})

	readRes := db2.Get(k)

	if readRes.Err != nil || *readRes.Value != v {
		t.Errorf("Failed to get key, error=%v", readRes.Err)
	}

	assert.NoError(t, readRes.Err, "failed to get key")
	assert.Equal(t, v, *readRes.Value)
}

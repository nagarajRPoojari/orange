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
)

func TestStorage_Get_Put(t *testing.T) {
	log.Disable()

	dbName := "test"
	dir := t.TempDir()
	const MEMTABLE_THRESHOLD = 1024 * 2

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	db := parrot.NewStorage[types.IntKey, types.IntValue](
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

	writeRes := db.Put(k, v)
	if writeRes.Err != nil {
		t.Errorf("Failed to put key, error=%v", writeRes.Err)
	}

	readRes := db.Get(k)

	if readRes.Err != nil || readRes.Value != v {
		t.Errorf("Failed to get key, error=%v", readRes.Err)
	}

}

func TestStorage_Load_DB(t *testing.T) {
	log.Disable()

	dbName := "test"
	dir := t.TempDir()

	const MEMTABLE_THRESHOLD = 1024 * 2

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	db1 := parrot.NewStorage[types.IntKey, types.IntValue](
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

	multiples := 10
	totalOps := int(MEMTABLE_THRESHOLD/v.SizeOf()) * multiples

	for i := range totalOps {
		db1.Put(types.IntKey{K: i}, types.IntValue{V: int32(i)})
	}

	time.Sleep(2 * time.Second)

	db2 := parrot.NewStorage[types.IntKey, types.IntValue](
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

	if readRes.Err != nil || readRes.Value != v {
		t.Errorf("Failed to get key, error=%v", readRes.Err)
	}

}

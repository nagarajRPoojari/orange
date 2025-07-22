// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package storage

import (
	"context"
	"testing"

	"github.com/nagarajRPoojari/orange/parrot/types"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
)

func TestStorage_Load(t *testing.T) {
	log.Disable()

	dbName := "test"

	const MEMTABLE_THRESHOLD = 1024 * 2

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	db := NewStorage[types.IntKey, types.IntValue](
		dbName,
		ctx,
		StorageOpts{
			Directory:         t.TempDir(),
			MemtableThreshold: MEMTABLE_THRESHOLD,
			TurnOnCompaction:  false,
			TurnOnWal:         false,
		})

	k, v := types.IntKey{K: 278}, types.IntValue{V: int32(267)}
	writeRes := db.Put(k, v)

	if writeRes.err != nil {
		t.Errorf("Failed to put key, error=%v", writeRes.err)
	}

	readRes := db.Get(k)

	if readRes.Err != nil || readRes.Value != v {
		t.Errorf("Failed to get key, error=%v", writeRes.err)
	}

}

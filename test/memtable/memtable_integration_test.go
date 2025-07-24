// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package memtable_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/nagarajRPoojari/orange/parrot/memtable"
	"github.com/nagarajRPoojari/orange/parrot/metadata"
	"github.com/nagarajRPoojari/orange/parrot/types"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
)

// TestMemtable_Write_And_Read_In_Mem verifies that a key-value pair
// written to the memtable can be read back correctly from memory,
// without triggering a flush to disk.
func TestMemtable_Write_And_Read_In_Mem(t *testing.T) {
	log.Disable()

	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: t.TempDir()})
	mts := memtable.NewMemtableStore[types.StringKey, *types.StringValue](
		mf,
		memtable.MemtableOpts{MemtableSoftLimit: 1024, QueueHardLimit: 10},
	)
	k, v := types.StringKey{K: "key-0"}, types.StringValue{V: "val-0"}
	mts.Write(k, &v)

	log.Infof("Write successfull")
	val, ok := mts.Read(k)

	if !ok || *val != v {
		t.Errorf("Expected %v, got %v", v, val)
	}
}

// TestMemtable_Write_Overflow_Trigger_Flush ensures that writing enough
// entries to exceed the memtable soft limit triggers a flush to disk.
// It then verifies that a previously written key can still be read back
// after clearing the in-memory state.
func TestMemtable_Write_Overflow_Trigger_Flush(t *testing.T) {
	log.Disable()

	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: t.TempDir()})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go mf.Sync(ctx)

	mts := memtable.NewMemtableStore[types.IntKey, *types.IntValue](
		mf,
		memtable.MemtableOpts{MemtableSoftLimit: 1024},
	)
	d := types.IntValue{V: 0}

	// overflow memtable to trigger flush
	for i := range int(1024 / d.SizeOf()) {
		mts.Write(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}

	k, v := types.IntKey{K: 90892389}, types.IntValue{V: 1993920}
	if ok := mts.Write(k, &v); !ok {
		t.Errorf("Expected to trigger flush")
	}

	// wait for memtable to flush & clear both memtable
	time.Sleep(3 * time.Second)

	mts.Clear()

	val, ok := mts.Read(types.IntKey{K: 244})
	v = types.IntValue{V: 244}

	if !ok || *val != v {
		t.Errorf("Expected %v, got %v", v, val)
	}
}

// TestMemtable_Write_With_Multiple_Reader verifies concurrent read access
// to keys written before and after a memtable flush. It ensures that flushed
// data can still be read concurrently by multiple readers.
// Note:
//
//   - memtable/sst size is set to 1kb
//   - max concurrent readers limited to 5000
func TestMemtable_Write_With_Multiple_Reader(t *testing.T) {
	log.Disable()

	const MEMTABLE_THRESHOLD = 1024
	const MAX_CONCURRENT_READ_ROUTINES = 5000

	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: t.TempDir()})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go mf.Sync(ctx)

	// overflow first memtable to trigger flush
	mts := memtable.NewMemtableStore[types.IntKey, *types.IntValue](
		mf,
		memtable.MemtableOpts{MemtableSoftLimit: MEMTABLE_THRESHOLD},
	)
	d := types.IntValue{V: 0}
	for i := range int(MEMTABLE_THRESHOLD / d.SizeOf()) {
		mts.Write(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}
	offset := int(MEMTABLE_THRESHOLD / d.SizeOf())
	for i := range int(MEMTABLE_THRESHOLD / d.SizeOf()) {
		mts.Write(types.IntKey{K: i + offset}, &types.IntValue{V: int32(i + offset)})
	}

	// A small gap to let it flush to disk & erase
	// further read should come from disk sst
	time.Sleep(3 * time.Second)
	wg := sync.WaitGroup{}

	mts.Clear()

	ticket := make(chan struct{}, MAX_CONCURRENT_READ_ROUTINES)
	for i := range int(MEMTABLE_THRESHOLD / d.SizeOf()) {
		wg.Add(1)
		ticket <- struct{}{} // acquire a ticket
		func(i int) {
			defer func() {
				<-ticket // release the ticket
				wg.Done()
			}()

			val, ok := mts.Read(types.IntKey{K: i})
			v := types.IntValue{V: int32(i)}
			if !ok || *val != v {
				t.Errorf("Expected %v, got %v", v, val)
			}
		}(i)
	}
	wg.Wait()

}

// TestMemtable_Intensive_Write_And_Read verifies heavy concurrent read access
// to keys written before and after a memtable flush.
// Similar to TestMemtable_Write_With_Multiple_Reader but with more load.
// Note:
//
//   - memtable/sst size is set to 1mb
//   - max concurrent readers limited to 5000
func TestMemtable_Intensive_Write_And_Read(t *testing.T) {
	log.Disable()

	const MEMTABLE_THRESHOLD = 1024 * 2
	const MAX_CONCURRENT_READ_ROUTINES = 500

	temp := t.TempDir()
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: temp})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go mf.Sync(ctx)

	// overflow first memtable to trigger flush
	mts := memtable.NewMemtableStore[types.IntKey, *types.IntValue](
		mf,
		memtable.MemtableOpts{MemtableSoftLimit: MEMTABLE_THRESHOLD},
	)
	d := types.IntValue{V: 0}

	multiples := 10
	totalOps := int(MEMTABLE_THRESHOLD/d.SizeOf()) * multiples

	for i := range totalOps {
		mts.Write(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}

	// A small gap to let it flush to disk & erase
	// further read should come from disk sst
	time.Sleep(1000 * time.Millisecond)
	wg := sync.WaitGroup{}

	ticket := make(chan struct{}, MAX_CONCURRENT_READ_ROUTINES)

	for i := range 10 {
		wg.Add(1)
		ticket <- struct{}{} // acquire a ticket
		go func(i int) {
			defer func() {
				<-ticket // release the ticket
				wg.Done()
			}()

			val, ok := mts.Read(types.IntKey{K: i})
			v := types.IntValue{V: int32(i)}
			if !ok || *val != v {
				t.Errorf("Expected %v, got %v", v, val)
			}
		}(i)
	}

	wg.Wait()
}

func TestMemtable_Rollback(t *testing.T) {
	log.Disable()

	const MEMTABLE_THRESHOLD = 1024 * 2
	const MAX_CONCURRENT_READ_ROUTINES = 500

	temp := t.TempDir()
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: temp})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go mf.Sync(ctx)

	mts := memtable.NewMemtableStore[types.IntKey, *types.IntValue](
		mf,
		memtable.MemtableOpts{
			MemtableSoftLimit: MEMTABLE_THRESHOLD,
			LogDir:            temp,
			TurnOnWal:         true,
		},
	)

	// this shouldn't trigger memtable overflow
	totalOps := 100
	for i := range totalOps {
		mts.Write(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}

	wg := sync.WaitGroup{}

	ticket := make(chan struct{}, MAX_CONCURRENT_READ_ROUTINES)

	// clear all data
	mts.Clear()

	time.Sleep(2 * time.Second)

	// rollback: expected to bring back all vanished data
	mts.RollbackAll()

	for i := range totalOps {
		wg.Add(1)
		ticket <- struct{}{} // acquire a ticket
		go func(i int) {
			defer func() {
				<-ticket // release the ticket
				wg.Done()
			}()

			val, ok := mts.Read(types.IntKey{K: i})
			v := types.IntValue{V: int32(i)}
			if !ok || *val != v {
				t.Errorf("Expected %v, got %v", v, val)
			}
		}(i)
	}

	wg.Wait()
}

func TestMemtable_Delete_In_Memory(t *testing.T) {
	log.Disable()

	const MEMTABLE_THRESHOLD = 1024 * 2
	const MAX_CONCURRENT_READ_ROUTINES = 500

	temp := t.TempDir()
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: temp})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go mf.Sync(ctx)

	mts := memtable.NewMemtableStore[types.IntKey, *types.IntValue](
		mf,
		memtable.MemtableOpts{
			MemtableSoftLimit: MEMTABLE_THRESHOLD,
			LogDir:            temp,
			TurnOnWal:         true,
		},
	)

	// Perform relatively fewer write operations to ensure both write/delete ops won't
	// trigger flusher
	totalOps := 100
	for i := range totalOps {
		mts.Write(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}

	// Deleting few key which already exists in-memory memtable
	delStart, delEnd := 25, 67
	for i := delStart; i <= delEnd; i++ {
		mts.Delete(types.IntKey{K: i}, &types.IntValue{})
	}

	wg := sync.WaitGroup{}

	ticket := make(chan struct{}, MAX_CONCURRENT_READ_ROUTINES)

	time.Sleep(2 * time.Second)

	for i := range totalOps {
		wg.Add(1)
		ticket <- struct{}{} // acquire a ticket
		go func(i int) {
			defer func() {
				<-ticket // release the ticket
				wg.Done()
			}()

			val, ok := mts.Read(types.IntKey{K: i})
			v := types.IntValue{V: int32(i)}

			// expected to be deleted
			if i >= delStart && i <= delEnd {
				if ok {
					t.Errorf("Expected to be deleted, found=%v", val)
				}
			} else {
				if !ok || *val != v {
					t.Errorf("Expected %v, got %v", v, val)
				}
			}
		}(i)
	}

	wg.Wait()
}

func TestMemtable_Delete_On_Disk(t *testing.T) {
	log.Disable()

	const MEMTABLE_THRESHOLD = 1024 * 2
	const MAX_CONCURRENT_READ_ROUTINES = 500

	temp := t.TempDir()
	mf := metadata.NewManifest("test", metadata.ManifestOpts{Dir: temp})
	mf.Load()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	go mf.Sync(ctx)

	// overflow first memtable to trigger flush
	mts := memtable.NewMemtableStore[types.IntKey, *types.IntValue](
		mf,
		memtable.MemtableOpts{MemtableSoftLimit: MEMTABLE_THRESHOLD},
	)
	d := types.IntValue{V: 0}

	multiples := 10
	totalOps := int(MEMTABLE_THRESHOLD/d.SizeOf()) * multiples

	// Perform a sequence of operations where delete actions are sandwiched between writes.
	// This pattern ensures the following:
	//   - Keys targeted for deletion are already persisted to disk from the initial write phase.
	//   - Delete operations themselves are flushed to disk by triggering more write traffic
	// afterward. - Subsequent reads on deleted keys should correctly return nothing, hitting the
	// tombstone logic.
	//
	// Steps:
	//   1. Write the first half of the keys to ensure they are flushed to disk.
	//   2. Delete a subset of those keys (from delStart to delEnd).
	//   3. Continue writing the second half of the keys to flush delete markers as well.
	for i := range totalOps / 2 {
		mts.Write(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}

	delStart, delEnd := 0, 10
	for i := delStart; i <= delEnd; i++ {
		mts.Delete(types.IntKey{K: i}, &types.IntValue{})
	}

	for i := totalOps / 2; i < totalOps; i++ {
		mts.Write(types.IntKey{K: i}, &types.IntValue{V: int32(i)})
	}

	// A small gap to let it flush to disk & erase
	// further read should come from disk sst
	time.Sleep(1000 * time.Millisecond)
	wg := sync.WaitGroup{}

	ticket := make(chan struct{}, MAX_CONCURRENT_READ_ROUTINES)

	for i := range 100 {
		wg.Add(1)
		ticket <- struct{}{} // acquire a ticket
		go func(i int) {
			defer func() {
				<-ticket // release the ticket
				wg.Done()
			}()

			val, ok := mts.Read(types.IntKey{K: i})
			v := types.IntValue{V: int32(i)}
			if i >= delStart && i <= delEnd {
				// These keys should have been deleted.
				if ok {
					t.Errorf("Expected to be deleted, found=%v", val)
				}
			} else {
				// These keys should exist and match the expected value.
				if !ok || *val != v {
					t.Errorf("Expected %v, got %v", v, val)
				}
			}
		}(i)
	}

	wg.Wait()
}

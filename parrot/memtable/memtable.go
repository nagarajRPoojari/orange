// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package memtable

import (
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/nagarajRPoojari/orange/parrot/errors"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	"github.com/nagarajRPoojari/orange/parrot/wal"

	v2 "github.com/nagarajRPoojari/orange/parrot/cache/v2"
	"github.com/nagarajRPoojari/orange/parrot/metadata"
	"github.com/nagarajRPoojari/orange/parrot/types"

	"github.com/nagarajRPoojari/orange/parrot/flags"
)

type MemtableOperation string

const (
	WriteOperation  MemtableOperation = "WRITE"
	DeleteOperation MemtableOperation = "DELETE"
)

type MemTableEvent[K types.Key, V types.Value] struct {
	Key   K
	Value V
	Op    MemtableOperation
}

type MemtableOpts struct {
	// Maximum in-memory size of a memtable before it's marked for flushing (in bytes)
	MemtableSoftLimit int64

	// Maximum number of memtables allowed in the flush queue before producers are blocked
	QueueHardLimit int

	// Threshold for starting to flush memtables proactively to avoid hitting the hard limit
	QueueSoftLimit int

	// Enables write-ahead logging for durability
	TurnOnWal bool

	// Directory path where WAL files will be stored
	LogDir string
}

type Memtable[K types.Key, V types.Value] struct {
	data map[K]V

	// RWMutex to prevent concurrent io
	mu   *sync.RWMutex
	opts MemtableOpts
	wal  *wal.WAL[MemTableEvent[K, V]]
}

// NewMemtable initializes a new Memtable instance.
// If WAL is enabled via options, it also creates a new WAL file for durability.
func NewMemtable[K types.Key, V types.Value](opts MemtableOpts) *Memtable[K, V] {
	var wl *wal.WAL[MemTableEvent[K, V]]

	if opts.TurnOnWal {
		logPath := filepath.Join(opts.LogDir, fmt.Sprintf("wal-%d.log", time.Now().UnixNano()))
		wl, _ = wal.NewWAL[MemTableEvent[K, V]](logPath)
	}

	return &Memtable[K, V]{
		data: map[K]V{},
		mu:   &sync.RWMutex{},
		opts: opts,
		wal:  wl,
	}
}

func (t *Memtable[K, V]) GetWall() *wal.WAL[MemTableEvent[K, V]] {
	return t.wal
}

// @todo: optimize
func (t *Memtable[K, V]) BuildPayloadList() ([]types.Payload[K, V], int64) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var pl []types.Payload[K, V]
	var size int64
	for k, v := range t.data {
		pl = append(pl, types.Payload[K, V]{Key: k, Val: v})
		size += int64(v.SizeOf())
	}
	sort.Slice(pl, func(i, j int) bool {
		return pl[i].Key.Less(pl[j].Key)
	})
	return pl, size
}

func (t *Memtable[K, V]) Write(key K, value V) bool {
	t.mu.Lock()
	defer func() {
		t.mu.Unlock()
		// log the write event to wal
		if t.opts.TurnOnWal {
			t.wal.Append(MemTableEvent[K, V]{Key: key, Value: value, Op: WriteOperation})
		}
	}()

	// check soft threshold
	if uintptr(len(t.data)+1)*value.SizeOf() > uintptr(t.opts.MemtableSoftLimit) {
		return false
	}
	t.data[key] = value
	return true
}

func (t *Memtable[K, V]) Delete(key K, tombstone V) {
	t.mu.Lock()
	defer func() {
		t.mu.Unlock()
		// log the delete event to wal
		if t.opts.TurnOnWal {
			t.wal.Append(MemTableEvent[K, V]{Key: key, Op: DeleteOperation})
		}
	}()

	if _, ok := t.data[key]; !ok {
		t.data[key] = tombstone
	}

	t.data[key].MarkDeleted()
}

func (t *Memtable[K, V]) Read(key K) (V, flags.Flag) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	val, ok := t.data[key]
	if !ok {
		return val, flags.KeyNotFoundFlag
	}
	if val.IsDeleted() {
		return val, flags.KeyDeletedFlag
	}

	return val, flags.KeyFoundFlag
}

type MemtableStore[K types.Key, V types.Value] struct {
	mf *metadata.Manifest

	// Flush queue that holds memtable nodes waiting to be flushed
	q *Queue[K, V]

	// The current active memtable used for writes (not yet ready for flush)
	mem *Memtable[K, V]

	// The queue node corresponding to the current active memtable
	// Used to update disposability (wraps MemtableStore.mem)
	memNode *Node[K, V]

	flusher *Flusher[K, V]

	// Cache for decoded values to speed up reads
	DecoderCache *v2.CacheManager[K, V]

	opts MemtableOpts
}

func NewMemtableStore[K types.Key, V types.Value](
	mf *metadata.Manifest,
	opts MemtableOpts,
) *MemtableStore[K, V] {
	q := NewQueue[K, V](QueueOpts{HardLimit: opts.QueueHardLimit})
	mem := NewMemtable[K, V](opts)
	node := NewNode(mem)

	// make head node non-disposable
	node.immutable.Lock()
	q.Push(node)

	flusher := NewFlusher(q, mf, FlusherOpts{})
	go flusher.Run()

	memStore := &MemtableStore[K, V]{
		mf:           mf,
		q:            q,
		mem:          mem,
		opts:         opts,
		flusher:      flusher,
		memNode:      node,
		DecoderCache: v2.NewCacheManager[K, V](),
	}

	memStore.RollbackAll()

	return memStore
}

func (t *MemtableStore[K, V]) RollbackAll() error {
	if !t.opts.TurnOnWal {
		return errors.WALDisablederr
	}

	// List all WAL log files in the LogDir
	files, err := filepath.Glob(filepath.Join(t.opts.LogDir, "*.log"))
	if err != nil {
		log.Infof("error listing WAL files: %v", err)
		return err
	}

	// sort logs to run writes in order
	sort.Slice(files, func(j, i int) bool {
		// extract the integer part from the filename
		getNum := func(path string) int64 {
			base := filepath.Base(path)
			var num int64
			fmt.Sscanf(base, "wal-%d.log", &num)
			return num
		}
		return getNum(files[i]) < getNum(files[j])
	})

	for _, file := range files {
		t.rollback(file)
	}

	return nil
}

func (t *MemtableStore[K, V]) rollback(file string) {
	events, err := wal.Replay[MemTableEvent[K, V]](file)
	if err != nil {
		return
	}
	for _, event := range events {

		switch event.Op {
		case WriteOperation:
			t.Write(event.Key, event.Value)
		case DeleteOperation:
			t.Delete(event.Key, event.Value)
		}

	}
}

// warning! : helper function for unit tests
func (t *MemtableStore[K, V]) Clear() {
	for k := range t.mem.data {
		delete(t.mem.data, k)
	}
	t.mem = NewMemtable[K, V](t.opts)
	t.memNode = NewNode(t.mem)

	t.memNode.immutable.Lock()
	t.q.Push(t.memNode)
}

// Write puts key[K], value[V]
// return value will be true if it triggers flush
func (t *MemtableStore[K, V]) Write(key K, value V) bool {
	if ok := t.mem.Write(key, value); !ok {
		log.Infof("Memtable overflow")

		// create new memtable with same options
		mem := NewMemtable[K, V](t.opts)
		node := NewNode(mem)

		// make current memtable non-disposable
		node.immutable.Lock()

		t.q.Push(node)
		mem.Write(key, value)

		// unlock previous memtable to allow dumping
		t.memNode.immutable.Unlock()

		// update current memtable
		t.memNode = node
		t.mem = mem

		return true
	}
	return false
}

// Read reads value for key[K] from memtable followed by ssts
func (t *MemtableStore[K, V]) Read(key K) (V, bool) {
	// Search backwards in Queue

	log.Infof("Started reading from memtables")
	var null V

	node := t.q.tail
	for node != nil {
		v, flag := node.mem.Read(key)
		if flag == flags.KeyFoundFlag {
			return v, true
		} else if flag == flags.KeyDeletedFlag {
			return null, false
		}
		node = node.Prev
	}

	log.Infof("Started reading from sst")

	// Search backward at all sst
	level, _ := t.mf.GetLSM().GetLevel(0)
	cnt := 0

	for level != nil {
		tbls := level.GetTables()
		index := 0
		sortedKeys := make([]int, len(tbls))
		for k := range tbls {
			sortedKeys[index] = k
			index++
		}

		sort.Slice(sortedKeys, func(i, j int) bool {
			return sortedKeys[i] > sortedKeys[j]
		})

		for _, mapKey := range sortedKeys {
			table := tbls[mapKey]
			val, err := t.DecoderCache.Get(table.DBPath, table.IndexPath, key)
			if err != nil {
				switch err.(type) {

				case errors.KeyNotFoundErr:
					continue
				case errors.KeyDeletederr:
					return val.Val, false
				}
			}

			if val.Key == key {
				return val.Val, true
			}

			if reflect.DeepEqual(val, types.Payload[K, V]{}) {
				continue
			}
		}
		cnt++
		level, _ = t.mf.GetLSM().GetLevel(cnt)
	}
	var empty V
	return empty, false
}

func (t *MemtableStore[K, V]) Delete(key K, tomstone V) error {
	t.mem.Delete(key, tomstone)
	return nil
}

// Expensive ops
func (t *MemtableStore[K, V]) ReadAll() ([]V, error) {
	// Search backwards in Queue

	log.Infof("Started reading from memtables")

	result := make([]V, 0)
	keysMap := make(map[K]struct{}, 0)

	node := t.q.tail
	for node != nil {

		node.mem.mu.Lock()
		for k, v := range node.mem.data {
			if v.IsDeleted() {
				keysMap[k] = struct{}{}
			} else {
				result = append(result, v)
			}
		}
		node.mem.mu.Unlock()
		node = node.Prev
	}

	log.Infof("Started reading from sst")

	// Search backward at all sst
	level, _ := t.mf.GetLSM().GetLevel(0)
	cnt := 0

	for level != nil {
		tbls := level.GetTables()
		index := 0
		sortedKeys := make([]int, len(tbls))
		for k := range tbls {
			sortedKeys[index] = k
			index++
		}

		sort.Slice(sortedKeys, func(i, j int) bool {
			return sortedKeys[i] > sortedKeys[j]
		})

		for _, mapKey := range sortedKeys {
			table := tbls[mapKey]
			val, _ := t.DecoderCache.GetFullPayload(table.DBPath, table.IndexPath)
			for _, p := range val {
				if p.Val.IsDeleted() {
					keysMap[p.Key] = struct{}{}
				} else {
					result = append(result, p.Val)
				}
			}
		}
		cnt++
		level, _ = t.mf.GetLSM().GetLevel(cnt)
	}

	return result, nil
}

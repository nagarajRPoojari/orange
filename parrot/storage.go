// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package storage

import (
	"context"
	"path/filepath"
	"time"

	v2 "github.com/nagarajRPoojari/orange/parrot/cache/v2"
	"github.com/nagarajRPoojari/orange/parrot/compactor"
	"github.com/nagarajRPoojari/orange/parrot/errors"
	"github.com/nagarajRPoojari/orange/parrot/memtable"
	"github.com/nagarajRPoojari/orange/parrot/metadata"
	"github.com/nagarajRPoojari/orange/parrot/types"
)

// StorageOpts defines configuration options for the storage engine.
type StorageOpts struct {
	// Root directory where all data (WALs, SSTables, manifests) will be stored
	Directory string

	// Memtable configuration
	// Threshold (in bytes) after which the active memtable is flushed to disk
	MemtableThreshold int
	// Maximum number of memtables allowed in flush queue before blocking writes
	QueueHardLimit int
	// Soft limit to trigger proactive flushing before hitting the hard limit
	QueueSoftLimit int
	// Flusher time interval
	FlushTimeInterval time.Duration
	// Enables WAL for durability of writes
	TurnOnMemtableWal bool
	// Write-Ahead Log configuration
	MemtableWALTimeInterval time.Duration
	// event channel size
	MemtableWALEventChSize int32
	// writer buffer size
	MemtableWALWriterBufferSize int
	// Directory path where WAL files will be stored
	MemtableWALLogDir string

	// Compaction configuration
	// Enables background compaction and garbage collection
	TurnOnCompaction bool
	// Soft size limit for level 0 (in bytes)
	Level0MaxSizeInBytes int64
	// Growth factor used to compute soft size limits for higher levels.
	// For level x: maxSize = Level0MaxSizeInBytes * max(x * growthFactor, 1)
	MaxSizeInBytesGrowthFactor int32
	// compaction time interval
	CompactionTimeInterval time.Duration
	// wal time interval
	CompactionWALTimeInterval time.Duration
	// event channel size
	CompactionWALEventChSize int32
	// writer buffer size
	CompactionWALWriterBufferSize int
	// Directory to store compaction-related WALs or logs
	compactionWALLogDir string
}

type Storage[K types.Key, V types.Value] struct {
	name     string
	store    *memtable.MemtableStore[K, V]
	manifest *metadata.Manifest

	reader *Reader[K, V]
	writer *Writer[K, V]

	// context for smooth teardown
	context context.Context

	opts *StorageOpts
}

func NewStorage[K types.Key, V types.Value](name string, ctx context.Context, opts StorageOpts) *Storage[K, V] {
	opts.compactionWALLogDir = filepath.Join(opts.Directory, "gc")
	opts.MemtableWALLogDir = filepath.Join(opts.Directory, "wal")

	v := &Storage[K, V]{name: name, context: ctx, opts: &opts}
	v.createOrLoadCollection()
	v.reader = NewReader(v.store, ReaderOpts{})
	v.writer = NewWriter(v.store, WriterOpts{})

	if opts.TurnOnCompaction {

		gc := compactor.NewGC(
			v.manifest,
			(*v2.CacheManager[K, V])(v.store.DecoderCache),
			&compactor.SizeTiredCompaction[K, V]{
				Opts: compactor.SizeTiredCompactionOpts{
					Level0MaxSizeInBytes:       opts.Level0MaxSizeInBytes,
					MaxSizeInBytesGrowthFactor: opts.MaxSizeInBytesGrowthFactor,
				},
			},
			compactor.GCOpts{
				TimeInterval:        opts.CompactionTimeInterval,
				WALTimeInterval:     opts.CompactionWALTimeInterval,
				WALEventChSize:      opts.CompactionWALEventChSize,
				WALWriterBufferSize: opts.CompactionWALWriterBufferSize,
			},
		)
		go gc.Run(ctx)
	}

	return v
}

func (t *Storage[K, V]) createOrLoadCollection() {
	mf := metadata.NewManifest(t.name, metadata.ManifestOpts{Dir: t.opts.Directory})
	mf.Load()

	mf.SyncLoop(t.context)

	mt := memtable.NewMemtableStore[K, V](
		mf,
		t.context,
		memtable.MemtableOpts{
			MemtableSoftLimit:   int64(t.opts.MemtableThreshold),
			QueueHardLimit:      t.opts.QueueHardLimit,
			QueueSoftLimit:      t.opts.QueueSoftLimit,
			WALLogDir:           t.opts.MemtableWALLogDir,
			WALTimeInterval:     t.opts.MemtableWALTimeInterval,
			WALEventChSize:      t.opts.MemtableWALEventChSize,
			WALWriterBufferSize: t.opts.MemtableWALWriterBufferSize,
			TurnOnWal:           t.opts.TurnOnMemtableWal,
			FlushTimeInterval:   t.opts.FlushTimeInterval,
		})
	t.store = mt
	t.manifest = mf
}

func (t *Storage[K, V]) Get(key K) ReadStatus[V] {
	return t.reader.Get(key)
}

func (t *Storage[K, V]) Put(key K, value V) WriteStatus {
	return t.writer.Put(key, value)
}

func (t *Storage[K, V]) Delete(key K, tomstone V) WriteStatus {
	return t.writer.Delete(key, tomstone)
}

type ReadStatus[V types.Value] struct {
	Value V
	Err   error
}

type ReaderOpts struct {
}

type Reader[K types.Key, V types.Value] struct {
	store *memtable.MemtableStore[K, V]

	opts ReaderOpts
}

func NewReader[K types.Key, V types.Value](store *memtable.MemtableStore[K, V], opts ReaderOpts) *Reader[K, V] {
	r := &Reader[K, V]{
		store: store,
		opts:  opts,
	}

	r.opts = opts

	return r
}

func (t *Reader[K, V]) Get(key K) ReadStatus[V] {
	val, ok := t.store.Read(key)
	if !ok {
		return ReadStatus[V]{Err: errors.RaiseKeyNotFoundErr("key=%v", key)}
	}
	return ReadStatus[V]{Value: val}
}

type WriteStatus struct {
	Err error
}

type WriterOpts struct {
}

type Writer[K types.Key, V types.Value] struct {
	store *memtable.MemtableStore[K, V]

	opts WriterOpts
}

func NewWriter[K types.Key, V types.Value](store *memtable.MemtableStore[K, V], opts WriterOpts) *Writer[K, V] {
	r := &Writer[K, V]{
		store: store,
		opts:  opts,
	}
	r.opts = opts

	return r
}

func (t *Writer[K, V]) Put(key K, value V) WriteStatus {
	_ = t.store.Write(key, value)
	return WriteStatus{Err: nil}
}

func (t *Writer[K, V]) Delete(key K, tomstone V) WriteStatus {
	_ = t.store.Delete(key, tomstone)
	return WriteStatus{Err: nil}
}

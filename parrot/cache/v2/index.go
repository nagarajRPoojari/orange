// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package v2

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"sync"

	fio "github.com/nagarajRPoojari/orange/parrot/io"
	"github.com/nagarajRPoojari/orange/parrot/types"
	"github.com/nagarajRPoojari/orange/parrot/utils"

	perrors "github.com/nagarajRPoojari/orange/parrot/errors"
)

// CacheManager caches SSTables, acts as only entrypoint
// to access SSTables
type CacheManager[K types.Key, V types.Value] struct {
	// using sync.Map to prevent race
	cache sync.Map
}

func NewCacheManager[K types.Key, V types.Value]() *CacheManager[K, V] {
	return &CacheManager[K, V]{
		cache: sync.Map{},
	}
}

// Get loads value for single key
func (m *CacheManager[K, V]) Get(dbPath string, indexPath string, key K) (types.Payload[K, V], error) {
	val, loaded := m.cache.Load(dbPath)
	var null types.Payload[K, V]
	if loaded {
		return val.(*CacheUnit[K, V]).GetDecodedForKey(key)
	}

	fm := fio.GetFileManager()
	dbFileReader, err := fm.OpenForRead(dbPath)
	if err != nil {
		return null, err
	}
	indexFileReader, err := fm.OpenForRead(indexPath)
	if err != nil {
		return null, err
	}

	// Create new cache and use LoadOrStore to avoid race
	newCache := &CacheUnit[K, V]{
		onceDecodeAllValues: sync.Once{},
		onceDecodeIndex:     sync.Once{},
		dbPayload:           dbFileReader.GetPayload(),
		indexPayload:        indexFileReader.GetPayload(),
	}
	actual, _ := m.cache.LoadOrStore(dbPath, newCache)

	return actual.(*CacheUnit[K, V]).GetDecodedForKey(key)
}

// GetFullPayload loads full payload list
func (m *CacheManager[K, V]) GetFullPayload(dbPath string, indexPath string) ([]types.Payload[K, V], error) {

	val, loaded := m.cache.Load(dbPath)
	if loaded {
		return val.(*CacheUnit[K, V]).getDecodedForAll()
	}

	fm := fio.GetFileManager()
	dbFileReader, err := fm.OpenForRead(dbPath)
	if err != nil {
		return nil, err
	}
	indexFileReader, err := fm.OpenForRead(indexPath)
	if err != nil {
		return nil, err
	}

	// Create new cache and use LoadOrStore to avoid race
	newCache := &CacheUnit[K, V]{
		onceDecodeAllValues: sync.Once{},
		onceDecodeIndex:     sync.Once{},
		dbPayload:           dbFileReader.GetPayload(),
		indexPayload:        indexFileReader.GetPayload(),
	}
	actual, _ := m.cache.LoadOrStore(dbPath, newCache)

	return actual.(*CacheUnit[K, V]).getDecodedForAll()

}

// CacheUnit holds data(index, data) related to single SSTable
type CacheUnit[K types.Key, V types.Value] struct {
	// dbpayload directly maps to data file mmap page (shared with multiple readers)
	dbPayload []byte

	// indexPayload directly maps to index file mmap page (shared with multiple readers)
	indexPayload []byte

	onceDecodeAllValues sync.Once
	onceDecodeIndex     sync.Once

	// decoded version of loaded payload
	indexDecoded []utils.IndexPayload[K, V]
	err          error
}

// loadIndex loads .index file and caches
//   - will be executed only once per cache unit
func (dc *CacheUnit[K, V]) loadIndex() {
	dc.onceDecodeIndex.Do(func() {
		// @todo: pre allocate
		var result []utils.IndexPayload[K, V]
		indexDecoder := gob.NewDecoder(bytes.NewReader(dc.indexPayload))

		for {
			var entry utils.IndexPayload[K, V]
			err := indexDecoder.Decode(&entry)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				dc.err = fmt.Errorf("failed to decode index: %w", err)
				return
			}
			result = append(result, entry)
		}

		dc.indexDecoded = result
	})
}

// GetDecodedForKey loads value for specific key
//   - @todo: caches loaded valu
//   - does binary search on index file to search for corresponding value offset
func (dc *CacheUnit[K, V]) GetDecodedForKey(key K) (types.Payload[K, V], error) {
	dc.loadIndex()

	if dc.err != nil {
		return types.Payload[K, V]{}, dc.err
	}

	left, mid, right := 0, 0, len(dc.indexDecoded)

	for left <= right {
		mid = left + (right-left)/2
		midK := dc.indexDecoded[mid]
		if midK.Key.Less(key) {
			left = mid + 1
		} else {
			if midK.Key == key {
				if int(midK.Offset+midK.Size) > len(dc.dbPayload) {
					return types.Payload[K, V]{}, perrors.IndexOutOfBoundErr("key=%v", key)
				}

				valDecoder := gob.NewDecoder(bytes.NewReader(dc.dbPayload[midK.Offset : midK.Offset+midK.Size]))
				var entry types.Payload[K, V]
				if err := valDecoder.Decode(&entry); err != nil {
					return types.Payload[K, V]{}, perrors.DecodeErr("key=%v, err=%v", key, err)
				}

				if entry.Val.IsDeleted() {
					fmt.Println("deleted key ", key)
					return entry, perrors.RaiseKeyDeletederr("key=%v", key)
				}

				return entry, nil
			} else {
				right = mid - 1
			}
		}
	}

	return types.Payload[K, V]{}, perrors.RaiseKeyNotFoundErr("key=%v", key)
}

// getDecodedForAll to load all entries of SSTable for compaction
//
//   - need not to be cached as, no reads are assumed to happen after compaction
//   - @todo: implicitly evict cache unit
func (dc *CacheUnit[K, V]) getDecodedForAll() ([]types.Payload[K, V], error) {
	dc.loadIndex()

	if dc.err != nil {
		return nil, dc.err
	}

	// @todo: pre allocate
	result := make([]types.Payload[K, V], 0)

	for _, k := range dc.indexDecoded {
		// Validate range
		if int(k.Offset+k.Size) > len(dc.dbPayload) {
			return nil, perrors.IndexOutOfBoundErr("key=%v", k.Key)
		}

		valDecoder := gob.NewDecoder(bytes.NewReader(dc.dbPayload[k.Offset : k.Offset+k.Size]))
		var entry types.Payload[K, V]
		if err := valDecoder.Decode(&entry); err != nil {
			return nil, perrors.DecodeErr("key=%v, err=%v", k.Key, err)
		}

		result = append(result, entry)
	}

	return result, nil
}

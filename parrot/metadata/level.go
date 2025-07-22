// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package metadata

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type Level struct {
	tables map[int]*SSTable

	SizeInBytes atomic.Int64

	// @todo: create separate locks for each field
	mu *sync.RWMutex
	// mutex to lock rw on levels, one must acquire respective level lock for
	// updating level
}

func NewLevel() *Level {
	return &Level{tables: map[int]*SSTable{}, mu: &sync.RWMutex{}, SizeInBytes: atomic.Int64{}}
}

func (t *Level) GetNextId() int {
	return int(time.Now().UnixNano())
}

func (t *Level) SetSSTable(i int, table *SSTable) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tables[i] = table
	t.SizeInBytes.Add(table.SizeInBytes)
}

func (t *Level) TablesCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.tables)
}

func (t *Level) GetTables() map[int]*SSTable {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.tables
}

func (t *Level) GetTable(i int) (*SSTable, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if len(t.tables) <= i || i < 0 {
		return nil, fmt.Errorf("failed to get index")
	}
	return t.tables[i], nil
}

// this wont release memory since reference to tables
// need to be returned
// !caution: clear mannualy for old
func (t *Level) Clear(ids []int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, i := range ids {
		t.SizeInBytes.Add(-t.tables[i].SizeInBytes)
		delete(t.tables, i)
	}

}

// Level snapshot
// Note: snapshots are immutable, exported fields are kept
//		 for json marshalling
// Warning!: it is not advised to modify snapshot views

type LevelView struct {
	Tables map[int]SSTableView `json:"tables"`
}

func NewLevelView() LevelView {
	return LevelView{Tables: map[int]SSTableView{}}
}

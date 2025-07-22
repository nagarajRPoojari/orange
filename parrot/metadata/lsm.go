// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package metadata

import (
	"fmt"
	"maps"
	"sync"
)

type LSM struct {
	name   string
	levels []*Level

	// @todo: create separate locks for each field
	mu *sync.RWMutex
	// mutex to lock rw on levels, one must acquire respective level lock for
	// updating level
}

func NewLSM(name string) *LSM {
	levels := make([]*Level, 0)
	levels = append(levels, NewLevel())
	return &LSM{name: name, levels: levels, mu: &sync.RWMutex{}}
}

func (t *LSM) LevelsCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.levels)
}

func (t *LSM) GetLevel(l int) (*Level, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if len(t.levels) <= l || l < 0 {
		return nil, fmt.Errorf("failed to get index")
	}
	return t.levels[l], nil
}

func (t *LSM) GetName() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.name
}

func (t *LSM) AppendLevel() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.levels = append(t.levels, NewLevel())
}

// LSM snapshot
// Note: snapshots are immutable, exported fields are kept
//		 for json marshalling
// Warning!: it is not advised to modify snapshot views

type LSMView struct {
	Name   string      `json:"name"`
	Levels []LevelView `json:"levels"`
}

func NewLSMView(name string) *LSMView {
	levels := make([]LevelView, 0)
	levels = append(levels, NewLevelView())
	return &LSMView{Name: name, Levels: levels}
}

// utility functions for serialization/deserialization between LSM instance & corresponding snapshot

func (view *LSMView) ToLSM() *LSM {
	lsm := &LSM{
		name:   view.Name,
		mu:     &sync.RWMutex{},
		levels: make([]*Level, len(view.Levels)),
	}

	for i, lvl := range view.Levels {
		lsm.levels[i] = lvl.Clone()
	}

	return lsm
}

func (lv LevelView) Clone() *Level {
	tables := make([]*SSTable, 0)
	for _, v := range lv.Tables {
		tbl := SSTable(v)
		tables = append(tables, &tbl)
	}
	tableMap := make(map[int]*SSTable, len(tables))
	for i, tbl := range tables {
		tableMap[i] = tbl
	}
	newLevel := NewLevel()
	newLevel.tables = tableMap

	return newLevel
}

func (lvl Level) Clone() *Level {
	newLevel := NewLevel()
	newLevel.tables = make(map[int]*SSTable, len(lvl.tables))
	maps.Copy(newLevel.tables, lvl.tables)
	return newLevel
}

func (lsm *LSM) ToView() *LSMView {
	lsm.mu.RLock()
	defer lsm.mu.RUnlock()

	view := &LSMView{
		Name:   lsm.name,
		Levels: make([]LevelView, len(lsm.levels)),
	}

	for i, lvl := range lsm.levels {
		view.Levels[i] = lvl.ToView()
	}

	return view
}

func (lvl *Level) ToView() LevelView {
	lvl.mu.RLock()
	defer lvl.mu.RUnlock()

	view := NewLevelView()
	view.Tables = make(map[int]SSTableView, len(lvl.GetTables()))
	for i, tb := range lvl.GetTables() {
		st := SSTableView(*tb)
		view.Tables[i] = st
	}

	return view
}

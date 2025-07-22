// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package metadata

type SSTable struct {
	DBPath      string
	IndexPath   string
	SizeInBytes int64
}

func NewSSTable(dBPath string, indexPath string, sizeInBytes int64) *SSTable {
	return &SSTable{DBPath: dBPath, IndexPath: indexPath, SizeInBytes: sizeInBytes}
}

// SSTable snapshot
// Note: snapshots are immutable, exported fields are kept
//		 for json marshalling
// Warning!: it is not advised to modify snapshot views

type SSTableView struct {
	DBPath      string `json:"dBPath"`
	IndexPath   string `json:"indexPath"`
	SizeInBytes int64  `json:"size"`
}

func NewSSTableView(DBPath string, IndexPath string, sizeInBytes int64) SSTable {
	return SSTable{DBPath: DBPath, IndexPath: IndexPath, SizeInBytes: sizeInBytes}
}

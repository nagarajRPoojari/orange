// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package io

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/nagarajRPoojari/orange/parrot/utils/log"

	"github.com/edsrzf/mmap-go"
)

type FileReader struct {
	payload mmap.MMap
	file    *os.File
}

func (t *FileReader) GetFile() *os.File {
	return t.file
}

func (t *FileReader) GetPayload() mmap.MMap {
	return t.payload
}

func (t *FileReader) Close() {
	t.file.Close()
	t.payload.Unmap()
}

type FileWriter struct {
	file *os.File
}

func (t *FileWriter) Truncate(size int64) error {
	if err := t.file.Truncate(size); err != nil {
		return err
	}

	_, err := t.file.Seek(0, 0)
	return err
}

func (t *FileWriter) GetFile() *os.File {
	return t.file
}

func (t *FileWriter) Close() {
	t.file.Close()
}

func (t *FileWriter) Write(data []byte) {
	_, err := t.file.Write(data)
	if err != nil {
		log.Fatalf("failed to write data, error=%v", err)
	}

	// Sync call to flush data to disk
	t.file.Sync()
}

type FileManager struct {
	sharedFileReadersMap sync.Map
	lockMap              sync.Map

	// globalMu prevents multiple goroutines creating same instance
	globalMu sync.Mutex
}

func newFileManager() *FileManager {
	return &FileManager{
		sharedFileReadersMap: sync.Map{},
		lockMap:              sync.Map{},
	}
}

var singleInstance *FileManager
var once sync.Once

func GetFileManager() *FileManager {
	once.Do(func() { singleInstance = newFileManager() })
	return singleInstance
}

func (t *FileManager) openForSharedRead(path string) (*FileReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s, error=%v", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get stats of file %s, error=%v", path, err)
	}

	// empty files can't be opened through mmap
	if info.Size() == 0 || info.IsDir() {
		return nil, fmt.Errorf(
			"invalid file for mmap: size=%d, isDir=%v",
			info.Size(),
			info.IsDir(),
		)
	}

	mmapData, err := mmap.Map(f, mmap.RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("unable to open mmap, error=%v", err)
	}

	fileReader := &FileReader{payload: mmapData, file: f}
	return fileReader, nil
}

func (t *FileManager) getOrCreateLock(path string) *sync.Mutex {
	t.globalMu.Lock()
	defer t.globalMu.Unlock()

	lock, ok := t.lockMap.Load(path)
	if !ok {
		lock = &sync.Mutex{}
		t.lockMap.LoadOrStore(path, lock)
	}

	return lock.(*sync.Mutex)
}

// OpenForRead utilizes mmap for multiple read only ops,
// mmap with SHARED_READ points to common page cache
//
// !Caution: might not in sync with data on disk
//
//   - if write is using mmap with COPY mode, safe read but
//     needs fsync() to keep it sync
//
//   - if write is using mmap with RDWR mode or without mmap, might update same
//     page cache, leading to torn or corrupt data
func (t *FileManager) OpenForRead(path string) (*FileReader, error) {
	lock := t.getOrCreateLock(path)
	lock.Lock()
	defer lock.Unlock()

	if reader, ok := t.sharedFileReadersMap.Load(path); ok {
		return reader.(*FileReader), nil
	}
	reader, err := t.openForSharedRead(path)
	if err != nil {
		return nil, err
	}
	t.sharedFileReadersMap.LoadOrStore(path, reader)

	return reader, nil
}

// OpenForWrite requires Close call to flush data to disk properly.
// Suitable for single write/dump
func (t *FileManager) OpenForWrite(path string) *FileWriter {
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatalf("failed to create directory %v: %v", dir, err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("failed to open file for writing %v", err)
	}

	return &FileWriter{file: f}
}

// OpenForAppend requires Close call to flush data to disk properly.
// Suitable for single write/dump
func (t *FileManager) OpenForAppend(path string) *FileWriter {
	dir := filepath.Dir(path)

	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatalf("failed to create directory %v: %v", dir, err)
	}

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("failed to open file for writing %v", err)
	}

	return &FileWriter{file: f}
}

// Exists checks whether the given file or directory exists on disk.
// Returns true if the path exists and is accessible, false otherwise.
func (t *FileManager) Exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// Delete removes the file or directory at the given path.
// Returns an error if the deletion fails.
func (t *FileManager) Delete(path string) error {
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("failed to delete %s", path)
	}
	return nil
}

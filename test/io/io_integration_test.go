// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package io_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/nagarajRPoojari/orange/parrot/io"
	"github.com/stretchr/testify/assert"
)

func TestFileManager_WriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test.txt")

	manager := io.GetFileManager()

	writer := manager.OpenForWrite(testFilePath)
	defer writer.Close()

	expected := []byte("Hello from FileWriter!\n")
	writer.Write(expected)

	reader, err := manager.OpenForRead(testFilePath)
	assert.NoError(t, err)
	defer reader.Close()

	got := reader.GetPayload()
	assert.Equal(t, string(expected), string(got))
}

func TestFileManager_MultipleReadsReturnSameInstance(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "shared.txt")

	os.WriteFile(testFilePath, []byte("shared read mmap"), 0644)

	manager := io.GetFileManager()

	r1, err := manager.OpenForRead(testFilePath)
	assert.NoError(t, err)
	r2, err := manager.OpenForRead(testFilePath)
	assert.NoError(t, err)

	assert.Equal(t, r1, r2)
	assert.Equal(t, string(r1.GetPayload()), string(r2.GetPayload()))
}

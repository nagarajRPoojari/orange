// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package io

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFileManager_WriteAndRead(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "test.txt")

	manager := GetFileManager()

	writer := manager.OpenForWrite(testFilePath)
	defer writer.Close()

	expected := []byte("Hello from FileWriter!\n")
	writer.Write(expected)

	reader, err := manager.OpenForRead(testFilePath)
	if err != nil {
		t.Errorf("Got unexpected error=%v", err)
	}
	defer reader.Close()

	got := reader.payload
	if string(got) != string(expected) {
		t.Errorf("Expected %q, got %q", expected, got)
	}
}

func TestFileManager_MultipleReadsReturnSameInstance(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "shared.txt")

	os.WriteFile(testFilePath, []byte("shared read mmap"), 0644)

	manager := GetFileManager()

	r1, err := manager.OpenForRead(testFilePath)
	if err != nil {
		t.Errorf("Got unexpected error=%v", err)
	}
	r2, err := manager.OpenForRead(testFilePath)
	if err != nil {
		t.Errorf("Got unexpected error=%v", err)
	}

	if r1 != r2 {
		t.Error("Expected same FileReader instance for shared read, got different instances")
	}

	if string(r1.payload) != string(r2.payload) {
		t.Errorf("Expected both payloads to be equal, go different payload data")
	}
}

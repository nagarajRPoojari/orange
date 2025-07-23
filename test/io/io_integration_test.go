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

	"github.com/nagarajRPoojari/orange/parrot/io"
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
	if err != nil {
		t.Errorf("Got unexpected error=%v", err)
	}
	defer reader.Close()

	got := reader.GetPayload()
	if string(got) != string(expected) {
		t.Errorf("Expected %q, got %q", expected, got)
	}
}

func TestFileManager_MultipleReadsReturnSameInstance(t *testing.T) {
	tmpDir := t.TempDir()
	testFilePath := filepath.Join(tmpDir, "shared.txt")

	os.WriteFile(testFilePath, []byte("shared read mmap"), 0644)

	manager := io.GetFileManager()

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

	if string(r1.GetPayload()) != string(r2.GetPayload()) {
		t.Errorf("Expected both payloads to be equal, go different payload data")
	}
}

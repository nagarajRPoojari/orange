// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package wal_test

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nagarajRPoojari/orange/parrot/wal"
	"github.com/stretchr/testify/assert"
)

type event struct {
	Data string `json:"Data"`
}

func TestWAL_Write(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "test.gob")
	wal, err := wal.NewWAL[event](logFile)
	assert.NoError(t, err)

	testEvent := event{Data: "test data"}
	wal.Append(testEvent)

	time.Sleep(10 * time.Millisecond)

	data, err := os.ReadFile(logFile)

	var decodedEvent event
	err = gob.NewDecoder(bytes.NewReader(data)).Decode(&decodedEvent)
	assert.NoError(t, err)
	assert.Equal(t, decodedEvent, testEvent)
}

func TestWAL_Replay(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "test.log")
	wl, err := wal.NewWAL[event](logFile)
	assert.NoError(t, err)

	testEvents := []event{}
	for i := range 10 {
		ev := event{Data: fmt.Sprintf("test-%d", i)}
		testEvents = append(testEvents, ev)
		wl.Append(ev)
	}

	time.Sleep(10 * time.Millisecond)

	events, err := wal.Replay[event](logFile)
	assert.NoError(t, err)
	assert.Equal(t, testEvents, events)
}

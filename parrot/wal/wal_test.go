// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package wal

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type event struct {
	Data string `json:"Data"`
}

func TestWAL_Write(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "test.gob")
	wal, err := NewWAL[event](logFile)
	if err != nil {
		t.Error(err)
	}

	testEvent := event{Data: "test data"}
	wal.Append(testEvent)

	time.Sleep(10 * time.Millisecond)

	data, err := os.ReadFile(logFile)
	if err != nil {
		t.Error(err)
	}

	var decodedEvent event
	err = gob.NewDecoder(bytes.NewReader(data)).Decode(&decodedEvent)
	if err != nil {
		t.Error(err)
	}
	if decodedEvent != testEvent {
		t.Errorf("expected %v, got %v", testEvent, decodedEvent)
	}
}

func TestWAL_Replay(t *testing.T) {
	logFile := filepath.Join(t.TempDir(), "test.log")
	wl, err := NewWAL[event](logFile)
	if err != nil {
		t.Error(err)
	}

	testEvents := []event{}
	for i := range 10 {
		ev := event{Data: fmt.Sprintf("test-%d", i)}
		testEvents = append(testEvents, ev)
		wl.Append(ev)
	}

	time.Sleep(10 * time.Millisecond)

	events, err := Replay[event](logFile)
	if err != nil {
		t.Error(err)
	}

	if !reflect.DeepEqual(events, testEvents) {
		t.Errorf("expected %v, got %v", testEvents, events)
	}
}

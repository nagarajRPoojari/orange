// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package wal

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"errors"
	"io"
	"time"

	"github.com/nagarajRPoojari/orange/parrot/utils/log"

	"sync"

	customerr "github.com/nagarajRPoojari/orange/parrot/errors"

	fio "github.com/nagarajRPoojari/orange/parrot/io"
)

type Event interface {
}

// WAL implements a Write-Ahead Log to ensure durability of events.
// It serializes events to disk before they are applied, allowing recovery after crashes.
type WAL[E Event] struct {

	// Path to the WAL file on disk
	path string

	// Channel for queuing events to be written asynchronously
	eventCh chan E

	// Channel used to signal WAL shutdown
	closeCh chan struct{}

	// WaitGroup to wait for all background goroutines to finish during shutdown
	wg sync.WaitGroup

	fileWriter     *fio.FileWriter
	encoder        *gob.Encoder
	bufferedWriter *bufio.Writer

	mu sync.Mutex
}

// NewWAL returns new WAL instance
func NewWAL[E Event](path string) (*WAL[E], error) {

	fm := fio.GetFileManager()
	fw := fm.OpenForAppend(path)

	bw := bufio.NewWriterSize(fw.GetFile(), 4*1024*1024)
	w := &WAL[E]{
		fileWriter:     fw,
		path:           path,
		eventCh:        make(chan E, 1024),
		closeCh:        make(chan struct{}),
		encoder:        gob.NewEncoder(bw),
		bufferedWriter: bw,
	}

	w.wg.Add(1)
	go w.run()

	return w, nil
}

// Replay loads logs from give path & rebuilds event list
func Replay[E Event](path string) ([]E, error) {
	fm := fio.GetFileManager()
	if !fm.Exists(path) {
		return nil, customerr.FileNotFoundError
	}

	fr, err := fm.OpenForRead(path)
	if err != nil {
		return nil, err
	}
	defer fr.Close()

	var events []E
	decoder := gob.NewDecoder(bytes.NewReader(fr.GetPayload()))

	for {
		var entry E
		err := decoder.Decode(&entry)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Errorf("failed to decode, err=%v", err)
		}
		events = append(events, entry)
	}

	return events, nil
}

func (w *WAL[E]) Append(entry E) {
	w.eventCh <- entry
}

func (t *WAL[E]) run() {
	flushTicker := time.NewTicker(100 * time.Microsecond)
	defer flushTicker.Stop()

	for {
		select {
		case event := <-t.eventCh:
			t.write(event)

		case <-flushTicker.C:
			if err := t.flush(); err != nil {
			}

		case <-t.closeCh:
			t.drain()
			t.flush() // final flush before exiting
			return
		}
	}
}

// write writes a single entry to the WAL[E].
func (t *WAL[E]) write(entry Event) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.encoder.Encode(entry); err != nil {
		log.Fatalf("failed to encode log event %v", err)
	}

}

func (t *WAL[E]) flush() error {
	if err := t.bufferedWriter.Flush(); err != nil {
		return err
	}
	return nil
}

func (t *WAL[E]) Truncate() {
	t.fileWriter.Truncate(0)
}

func (t *WAL[E]) Delete() {
	fm := fio.GetFileManager()
	fm.Delete(t.path)
}

func (t *WAL[E]) drain() {
	for {
		select {
		case event := <-t.eventCh:
			t.write(event)
		default:
			t.fileWriter.Close()
			return
		}
	}
}

// Close gracefully shuts down the WAL[E].
func (t *WAL[E]) Close() {
	close(t.closeCh)
	t.wg.Wait()
}

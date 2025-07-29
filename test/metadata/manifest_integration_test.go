// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package metadata_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nagarajRPoojari/orange/parrot/metadata"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	"github.com/stretchr/testify/assert"
)

func TestManifest_GetLevel(t *testing.T) {
	type fields struct {
		name string
		lsm0 *metadata.LSM
	}
	type args struct {
		l int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *metadata.Level
		wantErr bool
	}{
		{
			name:    "-1 level",
			fields:  fields{name: "test", lsm0: metadata.NewLSM("test")},
			args:    args{-1},
			want:    nil,
			wantErr: true,
		},
		{
			name:    "-1 level",
			fields:  fields{name: "test", lsm0: metadata.NewLSM("test")},
			args:    args{10},
			want:    nil,
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &metadata.Manifest{
				Name: tt.fields.name,
				LSM0: tt.fields.lsm0,
			}
			level, err := tr.GetLSM().GetLevel(tt.args.l)
			assert.Equal(t, tt.wantErr, err != nil, "Unexpected error state: %v", err)
			assert.Equal(t, tt.want, level, "Manifest.GetLevel() mismatch")
		})
	}
}

func TestManifest_Load(t *testing.T) {
	log.Disable()
	tmpDir := t.TempDir()

	const testName = "test-db"
	const manifestFile = "manifest.json"
	manifestPath := filepath.Join(tmpDir, "manifest", testName, manifestFile)

	m := metadata.NewManifest(testName, metadata.ManifestOpts{Dir: tmpDir})
	err := m.Load()
	assert.NoError(t, err)

	_, statErr := os.Stat(manifestPath)
	assert.NoError(t, statErr)

	assert.NotNil(t, m.LSM0)

	lsmDataView := metadata.NewLSMView(testName)
	lsmData := lsmDataView.ToLSM()

	jsonData, err := json.Marshal(lsmDataView)
	assert.NoError(t, err)
	err = os.WriteFile(manifestPath, jsonData, 0644)
	assert.NoError(t, err)

	m2 := metadata.NewManifest(testName, metadata.ManifestOpts{Dir: tmpDir})
	err = m2.Load()
	assert.NoError(t, err)
	assert.NotNil(t, m2.LSM0)
	assert.Equal(t, m2.LSM0, lsmData)

	time.Sleep(100 * time.Millisecond)
}

func TestManifest_Sync(t *testing.T) {
	log.Disable()
	tmpDir := t.TempDir()

	const testName = "test-db"
	const manifestFile = "manifest.json"
	manifestPath := filepath.Join(tmpDir, "manifest", testName, manifestFile)

	m := metadata.NewManifest(testName, metadata.ManifestOpts{Dir: tmpDir})
	err := m.Load()
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	m.SyncLoop(ctx)

	for range 10 {
		level, _ := m.LSM0.GetLevel(0)
		nextId := level.GetNextId()
		level.SetSSTable(nextId, metadata.NewSSTable("dummy", "dummy", 0))
	}

	time.Sleep(4 * time.Second)

	lsmDataView := metadata.NewLSMView(testName)
	jsonData, err := json.Marshal(lsmDataView)
	assert.NoError(t, err)
	err = os.WriteFile(manifestPath, jsonData, 0644)
	assert.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
}

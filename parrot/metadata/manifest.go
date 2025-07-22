// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"

	"github.com/nagarajRPoojari/orange/parrot/utils/log"

	"github.com/nagarajRPoojari/orange/parrot/io"
)

const (
	MANIFEST = "manifest"
)

type ManifestOpts struct {
	Dir string
}

type Manifest struct {
	name string
	lsm0 *LSM

	opts ManifestOpts
}

func NewManifest(name string, opts ManifestOpts) *Manifest {
	return &Manifest{name: name, lsm0: NewLSM(name), opts: opts}
}

func (t *Manifest) Load() error {
	filePath := path.Join(t.opts.Dir, MANIFEST, t.name, fmt.Sprintf("%s.json", MANIFEST))
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// create an empty LSM, take a snapshot & save
			lsm := NewLSM(t.name)
			lsmView := lsm.ToView()
			emptyData, _ := json.Marshal(lsmView)

			fm := io.GetFileManager()
			fw := fm.OpenForWrite(filePath)
			fw.Write(emptyData)
			fw.Close()

			t.lsm0 = lsm
			return nil
		} else {
			return err
		}
	}

	// load lsmview/snapshot to new LSM
	lsmView := NewLSMView(t.name)
	_ = json.Unmarshal(data, lsmView)
	t.lsm0 = lsmView.ToLSM()
	return nil
}

func (t *Manifest) Sync(ctx context.Context) error {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			filePath := path.Join(t.opts.Dir, MANIFEST, t.name, fmt.Sprintf("%s.json", MANIFEST))

			// load consistent manifest snapshot
			// reason: json needs struct to export fields with no locks
			//		   lsm is rw protected through locks, using lsm directly might lead to data race
			lsmView := t.lsm0.ToView()
			log.Infof("Manifest LSM: %+v. %p\n", t.lsm0.levels, t.lsm0.levels)

			data, err := json.Marshal(lsmView)
			if err != nil {
				return err
			}

			fw := io.GetFileManager().OpenForWrite(filePath)
			fw.Write(data)
			fw.Close()
		}
	}
}

func (t *Manifest) FormatDBPath(l, i int) string {
	if l < 0 || i < 0 {
		return ""
	}

	return path.Join(t.opts.Dir, t.lsm0.GetName(), fmt.Sprintf("level-%d", l), fmt.Sprintf("sst-%d.db", i))
}

func (t *Manifest) FormatIndexPath(l, i int) string {
	if l < 0 || i < 0 {
		return ""
	}

	return path.Join(t.opts.Dir, t.lsm0.GetName(), fmt.Sprintf("level-%d", l), fmt.Sprintf("sst-%d.index", i))
}

func (t *Manifest) FormatLevelPath(l int) string {
	if l < 0 {
		return ""
	}

	return path.Join(t.opts.Dir, t.lsm0.GetName(), fmt.Sprintf("level-%d", l))
}

func (t *Manifest) GetLSM() *LSM {
	// it is safe to return lsm instance, lock management is done by LSM itself
	return t.lsm0
}

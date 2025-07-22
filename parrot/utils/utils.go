// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package utils

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"
	"os"

	"github.com/nagarajRPoojari/orange/parrot/types"
)

type IndexPayload[K types.Key, V types.Value] struct {
	Key    K
	Offset int64
	Size   int64
}

func Encode[K types.Key, V types.Value](dbFile *os.File, indexFile *os.File, kv []types.Payload[K, V]) error {
	indexEncoder := gob.NewEncoder(indexFile)

	for _, item := range kv {
		offset, err := dbFile.Seek(0, io.SeekCurrent)
		if err != nil {
			return fmt.Errorf("failed to get offset: %w", err)
		}

		var buf bytes.Buffer
		tmpEncoder := gob.NewEncoder(&buf)
		if err := tmpEncoder.Encode(item); err != nil {
			return fmt.Errorf("failed to encode to buffer: %w", err)
		}

		n, err := dbFile.Write(buf.Bytes())
		if err != nil {
			return fmt.Errorf("failed to write to db file: %w", err)
		}

		indexPayload := IndexPayload[K, V]{
			Key:    item.Key,
			Offset: offset,
			Size:   int64(n),
		}
		if err := indexEncoder.Encode(indexPayload); err != nil {
			return fmt.Errorf("failed to encode index payload: %w", err)
		}
	}
	if _, err := dbFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to reset dbFile seek: %w", err)
	}
	if _, err := indexFile.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("failed to reset indexFile seek: %w", err)
	}
	return nil
}

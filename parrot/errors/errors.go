// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package errors

import "fmt"

const KeyNotFoundError = IO("Key not found")
const FileNotFoundError = IO("File not found")
const WALDisabledError = IO("WAL disabled")

type IO string

func (t IO) Error() string {
	return fmt.Sprintf("io err: %s", string(t))
}

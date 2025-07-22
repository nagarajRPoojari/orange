// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package log

import (
	"io"
	"log"
)

func init() {
	log.SetPrefix("[Parrot] ")
	log.SetFlags(log.Ldate | log.Ltime | log.LstdFlags)
}

func Infof(format string, args ...any) {
	log.Printf("[INFO] "+format, args...)
}

func Warnf(format string, args ...any) {
	log.Printf("[WARN] "+format, args...)
}

func Errorf(format string, args ...any) {
	log.Printf("[ERROR] "+format, args...)
}

func Fatalf(format string, args ...any) {
	log.Fatalf("[FATAL] "+format, args...)
}
func Panicf(format string, args ...any) {
	log.Panicf("[FATAL] "+format, args...)
}
func Disable() {
	log.SetOutput(io.Discard)
}

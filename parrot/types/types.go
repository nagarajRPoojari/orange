// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package types

import "unsafe"

// Key types
type Key interface {
	comparable
	Less(other any) bool
}

type IntKey struct {
	K int
}

func (t IntKey) Less(other any) bool {
	otherInt, ok := other.(IntKey)
	if !ok {
		return false
	}
	return t.K < otherInt.K
}

type StringKey struct {
	K string
}

func (t StringKey) Less(other any) bool {
	otherStr, ok := other.(StringKey)
	if !ok {
		return false
	}
	return t.K < otherStr.K
}

// Value types

type Value interface {
	SizeOf() uintptr
}

func SizeOfValue[V any](v V) uintptr {
	return unsafe.Sizeof(v)
}
func (t StringValue) SizeOf() uintptr {
	return uintptr(len(t.V))
}

type IntValue struct {
	V int32
}

func (t IntValue) SizeOf() uintptr {
	return 4
}

type StringValue struct {
	V string
}

// Payload

type Payload[K Key, V Value] struct {
	Key K
	Val V
}

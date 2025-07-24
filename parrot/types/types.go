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
	MarkDeleted()
	IsDeleted() bool
}

func SizeOfValue[V any](v V) uintptr {
	return unsafe.Sizeof(v)
}

type IntValue struct {
	V int32
	D bool
}

func (t *IntValue) SizeOf() uintptr {
	return 4
}

func (t *IntValue) MarkDeleted() {
	t.D = true
}

func (t *IntValue) IsDeleted() bool {
	return t.D
}

type StringValue struct {
	V string
	D bool
}

func (t *StringValue) MarkDeleted() {
	t.D = true
}

func (t *StringValue) IsDeleted() bool {
	return t.D
}

func (t *StringValue) SizeOf() uintptr {
	return uintptr(len(t.V))
}

// Payload

type Payload[K Key, V Value] struct {
	Key K
	Val V
}

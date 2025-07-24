// Copyright (c) 2025 Nagaraj Poojari
// SPDX-License-Identifier: MIT
//
// This file is part of: github.com/nagarajRPoojari/parrot
// Licensed under the MIT License.

package errors

import "fmt"

type KeyNotFoundErr string

func (t KeyNotFoundErr) Error() string {
	return fmt.Sprintf("key not found: %s", string(t))
}

func RaiseKeyNotFoundErr(msg string, args ...any) KeyNotFoundErr {
	return KeyNotFoundErr(fmt.Sprintf(msg, args...))
}

type KeyDeletederr string

func (t KeyDeletederr) Error() string {
	return fmt.Sprintf("key deleted: %s", string(t))
}

func RaiseKeyDeletederr(msg string, args ...any) KeyDeletederr {
	return KeyDeletederr(fmt.Sprintf(msg, args...))
}

type IOErr string

func (t IOErr) Error() string {
	return fmt.Sprintf("io err: %s", string(t))
}

func FileNotFounderr(msg string, args ...any) IOErr {
	return IOErr(fmt.Sprintf("file not found: "+msg, args...))
}

type WALErr string

func (t WALErr) Error() string {
	return fmt.Sprintf("WAL err: %s", string(t))
}

const WALDisablederr = WALErr("WAL disabled")

type SerializationErr string

func (t SerializationErr) Error() string {
	return fmt.Sprintf("serialization err: %s", string(t))
}

func DecodeErr(msg string, args ...any) SerializationErr {
	return SerializationErr(fmt.Sprintf("failed to decode: "+msg, args...))
}

type GeneralErr string

func (t GeneralErr) Error() string {
	return fmt.Sprintf("general err: %s", string(t))
}

func IndexOutOfBoundErr(msg string, args ...any) GeneralErr {
	return GeneralErr(fmt.Sprintf("index out of bound: "+msg, args...))
}

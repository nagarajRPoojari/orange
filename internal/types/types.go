package types

import "time"

// basic types

type _ID intKey

// int
type INT int64Value
type INT64 int64Value
type INT32 int32Value
type INT16 int16Value
type INT8 int8Value

// float
type FLOAT int64Value
type FLOAT32 float32Value
type FLOAT64 float64Value

// bool
type BOOL boolValue

// string
type STRING stringValue

// byte
type BYTE byteValue

// date & time
type DATE dateValue
type TIME timeValue
type DATETIME datetimeValue
type TIMESTAMP timestampValue

// decimal / numeric
type DECIMAL decimalValue
type NUMERIC numericValue

// uuid
type UUID uuidValue

// blob
type BLOB blobValue

type intKey struct {
	K int
}

func (t intKey) Less(other any) bool {
	otherInt, ok := other.(intKey)
	if !ok {
		return false
	}
	return t.K < otherInt.K
}

// int64
type int64Value struct {
	V int64
}

func (t int64Value) SizeOf() uintptr {
	return 8
}

// int32
type int32Value struct {
	V int32
}

func (t int32Value) SizeOf() uintptr {
	return 4
}

// int16
type int16Value struct {
	V int16
}

func (t int16Value) SizeOf() uintptr {
	return 2
}

// int8
type int8Value struct {
	V int8
}

func (t int8Value) SizeOf() uintptr {
	return 1
}

// float64
type float64Value struct {
	V float64
}

func (t float64Value) SizeOf() uintptr {
	return 8
}

// float32
type float32Value struct {
	V float32
}

func (t float32Value) SizeOf() uintptr {
	return 8
}

// bool
type boolValue struct {
	V bool
}

func (t boolValue) SizeOf() uintptr {
	return 8
}

type stringValue struct {
	V string
}

func (t stringValue) SizeOf() uintptr {
	return uintptr(len(t.V))
}

// byte
type byteValue struct {
	V byte
}

func (t byteValue) SizeOf() uintptr {
	return 1
}

type dateValue struct {
	V time.Time // usually stored as YYYY-MM-DD
}

func (t dateValue) SizeOf() uintptr {
	return 4 // approximate size for a date-only (year, month, day)
}

type timeValue struct {
	V time.Time // usually stored as HH:MM:SS
}

func (t timeValue) SizeOf() uintptr {
	return 3 // approximate size (hour, min, sec)
}

type datetimeValue struct {
	V time.Time
}

func (t datetimeValue) SizeOf() uintptr {
	return 8 // standard size (date + time), depending on DB
}

type timestampValue struct {
	V time.Time
}

func (t timestampValue) SizeOf() uintptr {
	return 8 // similar to datetime
}

type decimalValue struct {
	V string // to store arbitrary precision decimal as string
}

func (t decimalValue) SizeOf() uintptr {
	return uintptr(len(t.V)) // depends on precision
}

type numericValue struct {
	V string
}

func (t numericValue) SizeOf() uintptr {
	return uintptr(len(t.V))
}

type uuidValue struct {
	V [16]byte // UUID is 128 bits
}

func (t uuidValue) SizeOf() uintptr {
	return 16
}

type blobValue struct {
	V []byte
}

func (t blobValue) SizeOf() uintptr {
	return uintptr(len(t.V))
}

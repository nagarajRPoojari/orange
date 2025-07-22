package types

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

//go:generate go run gen_types.go

package types

import (
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/nagarajRPoojari/orange/internal/errors"
)

// basic types

type ID intKey

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
	K int64
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

func NewINT64(v interface{}) (int64Value, error) {
	var null int64Value
	switch val := v.(type) {
	case int64:
		return int64Value{V: val}, nil
	case int:
		return int64Value{V: int64(val)}, nil
	case float64:
		return int64Value{V: int64(val)}, nil
	default:
		return null, errors.TypeCastError(fmt.Sprintf("type cast failed for %v", v))
	}
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

func toInt64(v interface{}) (int64, error) {
	switch val := v.(type) {
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	case float64:
		return int64(val), nil
	default:
		return 0, errors.TypeCastError(fmt.Sprintf("expected int64-compatible value, got %T", v))
	}
}

func toInt32(v interface{}) (int32, error) {
	switch val := v.(type) {
	case int32:
		return val, nil
	case int:
		return int32(val), nil
	case float64:
		return int32(val), nil
	default:
		return 0, errors.TypeCastError(fmt.Sprintf("expected int32-compatible value, got %T", v))
	}
}

func toInt16(v interface{}) (int16, error) {
	switch val := v.(type) {
	case int16:
		return val, nil
	case int:
		if val < -32768 || val > 32767 {
			return 0, errors.TypeCastError(fmt.Sprintf("int value %d out of int16 range", val))
		}
		return int16(val), nil
	case float64:
		if val < -32768 || val > 32767 {
			return 0, errors.TypeCastError(fmt.Sprintf("float64 value %f out of int16 range", val))
		}
		return int16(val), nil
	default:
		return 0, errors.TypeCastError(fmt.Sprintf("expected int16-compatible value, got %T", v))
	}
}

func toInt8(v interface{}) (int8, error) {
	switch val := v.(type) {
	case int8:
		return val, nil
	case int:
		if val < -128 || val > 127 {
			return 0, errors.TypeCastError(fmt.Sprintf("int value %d out of int8 range", val))
		}
		return int8(val), nil
	case float64:
		if val < -128 || val > 127 {
			return 0, errors.TypeCastError(fmt.Sprintf("float64 value %f out of int8 range", val))
		}
		return int8(val), nil
	default:
		return 0, errors.TypeCastError(fmt.Sprintf("expected int8-compatible value, got %T", v))
	}
}

func toFloat64(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	default:
		return 0, errors.TypeCastError(fmt.Sprintf("expected float64-compatible value, got %T", v))
	}
}

func toFloat32(v interface{}) (float32, error) {
	switch val := v.(type) {
	case float32:
		return float32(val), nil
	case int:
		return float32(val), nil
	default:
		return 0, errors.TypeCastError(fmt.Sprintf("expected float32-compatible value, got %T", v))
	}
}

func toBool(v interface{}) (bool, error) {
	switch val := v.(type) {
	case bool:
		return val, nil
	case string:
		if val == "true" {
			return true, nil
		} else if val == "false" {
			return false, nil
		}
	}
	return false, errors.TypeCastError(fmt.Sprintf("expected bool-compatible value, got %T", v))
}

func toString(v interface{}) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case bool:
		return strconv.FormatBool(val), nil
	case int:
		return strconv.FormatInt(int64(val), 10), nil
	case []byte:
		return string(val), nil
	default:
		return "", errors.TypeCastError(fmt.Sprintf("expected string-compatible value, got %T", v))
	}
}

func toTime(v interface{}) (time.Time, error) {
	switch val := v.(type) {
	case time.Time:
		return val, nil
	case string:
		layouts := []string{
			time.RFC3339,
			"2006-01-02",
			"2006-01-02 15:04:05",
			"15:04:05",
		}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, val); err == nil {
				return t, nil
			}
		}
		return time.Time{}, errors.TypeCastError(fmt.Sprintf("cannot parse time string: %s", val))
	default:
		return time.Time{}, errors.TypeCastError(fmt.Sprintf("expected time-compatible value, got %T", v))
	}
}

// id

func ToID(v interface{}) (ID, error) {
	i, err := toInt64(v)
	if err != nil {
		return ID{}, err
	}
	return ID{K: i}, nil
}

// int types
func ToINT(v interface{}) (INT, error) {
	i, err := toInt64(v)
	if err != nil {
		return INT{}, err
	}
	return INT(int64Value{V: i}), nil
}

func ToINT64(v interface{}) (INT64, error) {
	i, err := toInt64(v)
	if err != nil {
		return INT64{}, err
	}
	return INT64(int64Value{V: i}), nil
}

func ToINT32(v interface{}) (INT32, error) {
	i, err := toInt32(v)
	if err != nil {
		return INT32{}, err
	}
	return INT32(int32Value{V: i}), nil
}

func ToINT16(v interface{}) (INT16, error) {
	i, err := toInt16(v)
	if err != nil {
		return INT16{}, err
	}
	return INT16(int16Value{V: i}), nil
}

func ToINT8(v interface{}) (INT8, error) {
	i, err := toInt8(v)
	if err != nil {
		return INT8{}, err
	}
	return INT8(int8Value{V: i}), nil
}

// float types
func ToFLOAT(v interface{}) (FLOAT, error) {
	i, err := toInt64(v)
	if err != nil {
		return FLOAT{}, err
	}
	return FLOAT(int64Value{V: i}), nil
}

func ToFLOAT32(v interface{}) (FLOAT32, error) {
	f, err := toFloat32(v)
	if err != nil {
		return FLOAT32{}, err
	}
	return FLOAT32(float32Value{V: f}), nil
}

func ToFLOAT64(v interface{}) (FLOAT64, error) {
	f, err := toFloat64(v)
	if err != nil {
		return FLOAT64{}, err
	}
	return FLOAT64(float64Value{V: f}), nil
}

// bool
func ToBOOL(v interface{}) (BOOL, error) {
	b, err := toBool(v)
	if err != nil {
		return BOOL{}, err
	}
	return BOOL(boolValue{V: b}), nil
}

// string
func ToSTRING(v interface{}) (STRING, error) {
	s, err := toString(v)
	if err != nil {
		return STRING{}, err
	}
	return STRING(stringValue{V: s}), nil
}

// date/time types
func ToDATE(v interface{}) (DATE, error) {
	t, err := toTime(v)
	if err != nil {
		return DATE{}, err
	}
	return DATE(dateValue{V: t}), nil
}

func ToTIME(v interface{}) (TIME, error) {
	t, err := toTime(v)
	if err != nil {
		return TIME{}, err
	}
	return TIME(timeValue{V: t}), nil
}

func ToDATETIME(v interface{}) (DATETIME, error) {
	t, err := toTime(v)
	if err != nil {
		return DATETIME{}, err
	}
	return DATETIME(datetimeValue{V: t}), nil
}

func ToTIMESTAMP(v interface{}) (TIMESTAMP, error) {
	t, err := toTime(v)
	if err != nil {
		return TIMESTAMP{}, err
	}
	return TIMESTAMP(timestampValue{V: t}), nil
}

// decimal/numeric
func ToDECIMAL(v interface{}) (DECIMAL, error) {
	var validDecimal = regexp.MustCompile(`^-?\d+(\.\d+)?$`)
	s, err := toString(v)
	if err != nil {
		return DECIMAL{}, err
	}

	if !validDecimal.MatchString(s) {
		return DECIMAL{}, fmt.Errorf("invalid decimal format: %q", s)
	}

	return DECIMAL(decimalValue{V: s}), nil
}

func ToNUMERIC(v interface{}) (NUMERIC, error) {
	s, err := toString(v)
	if err != nil {
		return NUMERIC{}, err
	}
	return NUMERIC(numericValue{V: s}), nil
}

// UUID
func ToUUID(v interface{}) (UUID, error) {
	switch val := v.(type) {
	case [16]byte:
		return UUID(uuidValue{V: val}), nil
	case string:
		// You can parse string UUID here if needed
		return UUID{}, errors.TypeCastError("UUID parsing from string not implemented")
	default:
		return UUID{}, errors.TypeCastError(fmt.Sprintf("expected UUID-compatible value, got %T", v))
	}
}

// BLOB
func ToBLOB(v interface{}) (BLOB, error) {
	switch val := v.(type) {
	case []byte:
		return BLOB(blobValue{V: val}), nil
	case string:
		return BLOB(blobValue{V: []byte(val)}), nil
	default:
		return BLOB{}, errors.TypeCastError(fmt.Sprintf("expected []byte-compatible value, got %T", v))
	}
}

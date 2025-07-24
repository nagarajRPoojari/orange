//go:generate go run gen_types.go

package types

import (
	"fmt"
	"strconv"
	"time"

	"encoding/gob"

	"github.com/nagarajRPoojari/orange/internal/errors"
)

func init() {
	// Common map/slice types
	gob.Register(map[string]interface{}{})
	gob.Register([]interface{}{})

	// Basic scalar types
	gob.Register("")         // string
	gob.Register(0)          // int
	gob.Register(float64(0)) // float64
	gob.Register(true)       // bool

	// Time-related types
	gob.Register(time.Time{})

	gob.Register(INT(0))
	gob.Register(INT64(0))
	gob.Register(INT32(0))
	gob.Register(INT16(0))
	gob.Register(INT8(0))

	// Float types
	gob.Register(FLOAT(0))
	gob.Register(FLOAT32(0))
	gob.Register(FLOAT64(0))

	// Bool and string
	gob.Register(BOOL(false))
	gob.Register(STRING(""))

	// Date & time-related
	gob.Register(DATE{})
	gob.Register(TIME{})
	gob.Register(DATETIME{})
	gob.Register(TIMESTAMP{})

}

// basic types

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
// type DECIMAL decimalValue
// type NUMERIC numericValue

// // uuid
// type UUID uuidValue

// // blob
// type BLOB blobValue

type ID struct {
	K int64
}

func (t ID) Less(other any) bool {
	otherInt, ok := other.(ID)
	if !ok {
		return false
	}
	return t.K < otherInt.K
}

// int64
type int64Value int64

// int32
type int32Value int32

// int16
type int16Value int16

// int8
type int8Value int8

// float64
type float64Value float64

// float32
type float32Value float32

// bool
type boolValue bool

type stringValue string

// byte
type byteValue byte

type dateValue time.Time // usually stored as YYYY-MM-DD

type timeValue time.Time // usually stored as HH:MM:SS

type datetimeValue time.Time

type timestampValue time.Time

// type decimalValue string // to store arbitrary precision decimal as string

// type numericValue string

// type uuidValue [16]byte // UUID is 128 bits

// type blobValue []byte

func toid(v interface{}) (int64, error) {
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

func ToID(v interface{}) (int64, error) {
	return toid(v)
}

func toint64(v interface{}) (int64, error) {
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

func toint(v interface{}) (int, error) {
	switch val := v.(type) {
	case int:
		return int(val), nil
	case float64:
		return int(val), nil
	default:
		return 0, errors.TypeCastError(fmt.Sprintf("expected int-compatible value, got %T", v))
	}
}

func todate(v interface{}) (time.Time, error) {
	switch val := v.(type) {
	case time.Time:
		return val, nil
	case string:
		// Try common layouts
		layouts := []string{
			time.RFC3339,
			"2006-01-02",
			"2006-01-02 15:04:05",
		}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, val); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("unable to parse string to time: %q", val)
	case float64:
		// Assume it's a Unix timestamp in seconds
		return time.Unix(int64(val), 0), nil
	case int64:
		return time.Unix(val, 0), nil
	case int:
		return time.Unix(int64(val), 0), nil
	default:
		return time.Time{}, fmt.Errorf("expected time-compatible value, got %T", v)
	}
}

func toint32(v interface{}) (int32, error) {
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

func toint16(v interface{}) (int16, error) {
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

func toint8(v interface{}) (int8, error) {
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

func tofloat64(v interface{}) (float64, error) {
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
func tofloat(v interface{}) (float64, error) {
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

func tofloat32(v interface{}) (float32, error) {
	switch val := v.(type) {
	case float32:
		return float32(val), nil
	case int:
		return float32(val), nil
	default:
		return 0, errors.TypeCastError(fmt.Sprintf("expected float32-compatible value, got %T", v))
	}
}

func tobool(v interface{}) (bool, error) {
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

func tostring(v interface{}) (string, error) {
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

func totime(v interface{}) (time.Time, error) {
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

func todatetime(v interface{}) (time.Time, error) {
	switch val := v.(type) {
	case time.Time:
		return val, nil
	case string:
		// Try common date-time layouts
		layouts := []string{
			time.RFC3339,
			"2006-01-02 15:04:05",
			"2006-01-02T15:04:05",
			"2006-01-02",
		}
		for _, layout := range layouts {
			if t, err := time.Parse(layout, val); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("unable to parse string to datetime: %q", val)
	case float64:
		return time.Unix(int64(val), 0), nil
	case int64:
		return time.Unix(val, 0), nil
	case int:
		return time.Unix(int64(val), 0), nil
	default:
		return time.Time{}, fmt.Errorf("expected datetime-compatible value, got %T", v)
	}
}

// totimestamp converts supported input to a Unix timestamp in seconds.
func totimestamp(v interface{}) (int64, error) {
	switch val := v.(type) {
	case time.Time:
		return val.Unix(), nil
	case string:
		t, err := todatetime(val)
		if err != nil {
			return 0, fmt.Errorf("cannot convert string to timestamp: %w", err)
		}
		return t.Unix(), nil
	case float64:
		return int64(val), nil
	case int64:
		return val, nil
	case int:
		return int64(val), nil
	default:
		return 0, fmt.Errorf("expected timestamp-compatible value, got %T", v)
	}
}

// int types
func ToINT(v interface{}) (INT, error) {
	i, err := toint64(v)
	if err != nil {
		return INT(0), err
	}
	return INT(i), nil
}

func ToINT64(v interface{}) (INT64, error) {
	i, err := toint64(v)
	if err != nil {
		return INT64(0), err
	}
	return INT64(i), nil
}

func ToINT32(v interface{}) (INT32, error) {
	i, err := toint32(v)
	if err != nil {
		return INT32(0), err
	}
	return INT32(i), nil
}

func ToINT16(v interface{}) (INT16, error) {
	i, err := toint16(v)
	if err != nil {
		return INT16(0), err
	}
	return INT16(i), nil
}

func ToINT8(v interface{}) (INT8, error) {
	i, err := toint8(v)
	if err != nil {
		return INT8(0), err
	}
	return INT8(i), nil
}

// float types
func ToFLOAT(v interface{}) (FLOAT, error) {
	i, err := toint64(v)
	if err != nil {
		return FLOAT(0), err
	}
	return FLOAT(i), nil
}

func ToFLOAT32(v interface{}) (FLOAT32, error) {
	f, err := tofloat32(v)
	if err != nil {
		return FLOAT32(0), err
	}
	return FLOAT32(f), nil
}

func ToFLOAT64(v interface{}) (FLOAT64, error) {
	f, err := tofloat64(v)
	if err != nil {
		return FLOAT64(0), err
	}
	return FLOAT64(f), nil
}

// bool
func ToBOOL(v interface{}) (BOOL, error) {
	b, err := tobool(v)
	if err != nil {
		return BOOL(false), err
	}
	return BOOL(b), nil
}

// string
func ToSTRING(v interface{}) (STRING, error) {
	s, err := tostring(v)
	if err != nil {
		return STRING(""), err
	}
	return STRING(s), nil
}

// date/time types
func ToDATE(v interface{}) (DATE, error) {
	t, err := totime(v)
	if err != nil {
		return DATE{}, err
	}
	return DATE(t), nil
}

func ToTIME(v interface{}) (TIME, error) {
	t, err := totime(v)
	if err != nil {
		return TIME{}, err
	}
	return TIME(t), nil
}

func ToDATETIME(v interface{}) (DATETIME, error) {
	t, err := totime(v)
	if err != nil {
		return DATETIME{}, err
	}
	return DATETIME(t), nil
}

func ToTIMESTAMP(v interface{}) (TIMESTAMP, error) {
	t, err := totime(v)
	if err != nil {
		return TIMESTAMP{}, err
	}
	return TIMESTAMP(t), nil
}

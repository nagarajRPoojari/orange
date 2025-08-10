package utils

import (
	"os"
	"strconv"

	"github.com/nagarajRPoojari/orange/parrot/utils/log"
)

func GetEnv[T any](key string, fallback T, logFallback ...bool) T {
	if value, exists := os.LookupEnv(key); exists {
		var result any
		var err error

		switch any(fallback).(type) {
		case string:
			result = value
		case int:
			result, err = strconv.Atoi(value)
		case int64:
			result, err = strconv.ParseInt(value, 10, 64)
		case bool:
			result, err = strconv.ParseBool(value)
		case float64:
			result, err = strconv.ParseFloat(value, 64)
		default:
			log.Fatalf("unsupported type for key %s", key)
		}

		if err != nil {
			log.Fatalf("error parsing %s: %v", key, err)
		}

		return result.(T)
	}

	if len(logFallback) > 0 && logFallback[0] {
		log.Fatalf("%s must be provided", key)
	} else {
		log.Warnf("%s not set, using fallback `%v`", key, fallback)
	}
	return fallback
}

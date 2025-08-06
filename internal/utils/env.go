package utils

import (
	"os"

	"github.com/nagarajRPoojari/orange/parrot/utils/log"
)

func GetEnv(key string, fallback string, logFallback ...bool) string {
	if value, exists := os.LookupEnv(key); exists {
		log.Infof("%s=%s", key, value)
		return value
	}
	if len(logFallback) > 0 && logFallback[0] {
		log.Fatalf("%s must be provided", key)
	}
	log.Warnf("%s not set, using `%s`", key, fallback)
	return fallback
}

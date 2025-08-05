package utils

import (
	"os"

	"github.com/nagarajRPoojari/orange/parrot/utils/log"
)

func GetEnv(key string, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		log.Infof("%s=`%s`", key, fallback)
		return value
	}
	log.Warnf("%s not set, using `%s`", key, fallback)
	return fallback
}

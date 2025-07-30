package conf

import (
	"os"
	"time"
)

var DefaultGCDir = os.TempDir()

const DefaultWALTimeInterval = 100 * time.Microsecond
const DefaultWALEventBufferSize = 1024
const DefaultWriterBufferSize = 4 * 1024 * 1024

const DefaultCompactionTimeInterval = 1000 * time.Millisecond
const DefaultFlusherTimeInterval = 1000 * time.Millisecond

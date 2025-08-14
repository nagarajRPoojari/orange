package storage

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
)

type BenchmarkReport struct {
	TotalOps                   int     `json:"total_ops"`
	PayloadSize                int     `json:"payload_size"`
	TotalBytesTransferred      float64 `json:"total_bytes_transferred"`
	TotalTimeTaken             float64 `json:"total_time_taken_in_seconds"`
	OpsPerSec                  float64 `json:"ops_per_sec"`
	MegaBytesTransferredPerSec float64 `json:"megabytes_transferred_per_sec"`
	AverageLatency             float64 `json:"average_latency_in_ms"`
	SystemSpecs                SysInfo `json:"system_specs"`
}

type SysInfo struct {
	GOOS      string `json:"go_os"`
	GOARCH    string `json:"go_arch"`
	NumCPU    int    `json:"cpu_cores"`
	GoVersion string `json:"go_version"`
}

func getSystemInfo() SysInfo {
	return SysInfo{
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
		NumCPU:    runtime.NumCPU(),
		GoVersion: runtime.Version(),
	}
}

func (t BenchmarkReport) Dump(fname string) error {
	t.SystemSpecs = getSystemInfo()

	f, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	bytes, err := json.MarshalIndent(t, " ", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	bytes = append(bytes, '\n')

	_, err = f.Write(bytes)
	if err != nil {
		return fmt.Errorf("failed to write to file: %w", err)
	}

	return nil
}

func RandomKey(a, b int) int {
	return rand.Intn(b-a) + a
}

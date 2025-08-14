package storage

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
)

type BenchmarkReport struct {
	Name                      string  `json:"name"`
	TotalOps                  int     `json:"total_ops"`
	PayloadSizeInBytes        int     `json:"payload_size_in_bytes"`
	TotalDataTransferredInMB  float64 `json:"total_data_transferred_in_mb"`
	TotalTimeTakenInSec       float64 `json:"total_time_taken_in_seconds"`
	OpsPerSec                 float64 `json:"ops_per_sec"`
	DataTransferredInMBPerSec float64 `json:"data_transferred_in_mb_per_sec"`
	AverageLatencyInMicroSec  float64 `json:"average_latency_in_ms"`
	SystemSpecs               SysInfo `json:"system_specs"`
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

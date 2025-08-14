/*
Copyright © 2025 nagarajRPoojari

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
*/

package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"text/tabwriter"

	storage "github.com/nagarajRPoojari/orange/benchmark"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	"github.com/spf13/cobra"
)

var reportCmd = &cobra.Command{
	Use:   "report",
	Short: "Display generated benchmark report",
	Run: func(cmd *cobra.Command, args []string) {
		benchDir, _ := os.ReadDir("./benchmark")
		reports := []storage.BenchmarkReport{}
		for _, entry := range benchDir {
			if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
				fp := path.Join("./benchmark", entry.Name())
				b, err := os.ReadFile(fp)
				if err != nil {
					log.Errorf("failed to read file=%s, err=%v", fp, err)
					break
				}
				report := storage.BenchmarkReport{}
				err = json.Unmarshal(b, &report)
				if err != nil {
					log.Errorf("failed to decode file=%s, err=%v", fp, err)
					break
				}
				reports = append(reports, report)
			}
		}
		printBenchmarkReports(reports)
	},
}

func init() {
	rootCmd.AddCommand(reportCmd)
}

func printBenchmarkReports(reports []storage.BenchmarkReport) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', tabwriter.Debug)

	// Header
	fmt.Fprintln(w, "Name\tTotalOps\tPayload\tMB/s\tOps/s\tAvg Lat(micro sec)\tTime(s)\tGOOS\tARCH\tCPUs\tGoVersion")

	for _, r := range reports {
		fmt.Fprintf(w, "%s\t%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%s\t%s\t%d\t%s\n",
			r.Name,
			r.TotalOps,
			r.PayloadSizeInBytes,
			r.DataTransferredInMBPerSec,
			r.OpsPerSec,
			r.AverageLatencyInMicroSec,
			r.TotalTimeTakenInSec,
			r.SystemSpecs.GOOS,
			r.SystemSpecs.GOARCH,
			r.SystemSpecs.NumCPU,
			r.SystemSpecs.GoVersion,
		)
	}

	w.Flush()
}

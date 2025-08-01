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
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/nagarajRPoojari/orange/net/client"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	"github.com/nagarajRPoojari/orange/pkg/query"
	"github.com/spf13/cobra"
)

var (
	Port    string
	Address string
)

func processQuery(client *client.Client, q string) (any, error) {
	parser := query.NewParser(q)
	op, err := parser.Build()
	if err != nil {
		return nil, err
	}

	switch v := op.(type) {
	case query.CreateOp:
		return nil, client.Create(&v)
	case query.InsertOp:
		return nil, client.Insert(&v)
	case query.SelectOp:
		return client.Select(&v)
	case query.DeleteOp:
		return nil, fmt.Errorf("delete op not implpemented")
	}

	return nil, fmt.Errorf("syntax error: invalid op")
}

// replCmd represents the repl command
var replCmd = &cobra.Command{
	Use:   "repl",
	Short: "cli client to play with orangedb",
	Run: func(cmd *cobra.Command, args []string) {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("# orange repl")

		p, _ := strconv.ParseInt(Port, 10, 0)
		cl := client.NewClient(Address, p)

		log.Disable()
		for {
			fmt.Print("> ")
			input, err := reader.ReadString('\n')
			if err != nil {
				fmt.Println("Error reading input:", err)
				continue
			}

			input = strings.TrimSpace(input)

			if input == "exit" || input == "quit" {
				fmt.Println("# bye...")
				break
			}

			res, err := processQuery(cl, input)
			if err != nil {
				fmt.Printf("%v\n", err)
			} else {
				var data interface{}
				resBytes := res.([]byte)
				if err := json.Unmarshal(resBytes, &data); err != nil {
					fmt.Printf("Failed to unmarshal: %v\n", err)
					return
				}

				pretty, err := json.MarshalIndent(data, "", "  ")
				if err != nil {
					fmt.Printf("Failed to format: %v\n", err)
					return
				}
				fmt.Println(string(pretty))
			}

		}
	},
}

func init() {
	rootCmd.AddCommand(replCmd)

	replCmd.Flags().StringVarP(&Port, "port", "p", "8080", "Server port to connect to")
	replCmd.Flags().StringVarP(&Address, "address", "a", "127.0.0.1", "Server address to connect to")
}

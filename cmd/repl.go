/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
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

	replCmd.Flags().StringVarP(&Port, "port", "p", "8080", "Port to run the server on")
	replCmd.Flags().StringVarP(&Address, "address", "a", "127.0.0.1", "Address to bind the server to")
}

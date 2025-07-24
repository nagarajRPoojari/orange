package cmd

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	odb "github.com/nagarajRPoojari/orange/internal/db"
	"github.com/nagarajRPoojari/orange/parrot/utils/log"
	"github.com/spf13/cobra"
)

var replCmd = &cobra.Command{

	Use:   "repl",
	Short: "Start a REPL-style input loop",
	Run: func(cmd *cobra.Command, args []string) {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("# orange repl")

		db := odb.NewOrangedb(odb.DBopts{Dir: "./temp"})
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

			res, err := db.ProcessQuery(input)
			if err != nil {
				fmt.Printf("%v\n", err)
			} else {
				prettyJSON, err := json.MarshalIndent(res, "", "  ")
				if err != nil {
					fmt.Printf("Error formatting JSON: %v\n", err)
				} else {
					fmt.Println(string(prettyJSON))
				}
			}

		}
	},
}

func init() {
	rootCmd.AddCommand(replCmd)
}

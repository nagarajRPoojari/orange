package cmd

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/nagarajRPoojari/orange/net/server"
	"github.com/spf13/cobra"
)

var (
	ServerPort    string
	ServerAddress string
)

// serverCmd represents the server command
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run the server",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("Starting server at %s:%s...\n", ServerAddress, ServerPort)
		// log.Disable()
		stop := make(chan os.Signal, 1)
		signal.Notify(stop, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)

		p, _ := strconv.ParseInt(ServerPort, 10, 0)
		s := server.NewServer(ServerAddress, p)
		go s.Run()

		<-stop
		fmt.Println("\nShutting down server...")

		s.Stop()
	},
}

func init() {
	rootCmd.AddCommand(serverCmd)

	// Add flags
	serverCmd.Flags().StringVarP(&ServerPort, "port", "p", "8080", "Port to run the server ")
	serverCmd.Flags().StringVarP(&ServerAddress, "address", "a", "127.0.0.1", "Address to bind the server to")
}

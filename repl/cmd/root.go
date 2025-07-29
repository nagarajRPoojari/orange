/*
Copyright © 2025 nagarajRPoojari np137270@gmail.com
*/
package cmd

import (
	"os"
)

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := replCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	replCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

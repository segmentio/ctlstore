package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var binaryName = os.Args[0]

var rootCmd = &cobra.Command{
	Use:   binaryName,
	Short: fmt.Sprintf("%s allows an operator to query a ctlstore system", binaryName),
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Usage()
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

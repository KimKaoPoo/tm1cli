package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print version information",
	Long:  "Print tm1cli version information.",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("tm1cli v%s\n", Version)
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}

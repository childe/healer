package cmd

import (
	"fmt"
	"runtime"

	"github.com/spf13/cobra"
)

var (
	version   string
	buildTime string
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the version of healer",

	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("healer: %s compiled at %s with %v on %v/%v\n", version, buildTime, runtime.Version(), runtime.GOOS, runtime.GOARCH)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)
}

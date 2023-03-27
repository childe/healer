/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"flag"
	"os"

	"github.com/golang/glog"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "healer",
	Short: "kafka tool. you can use it to consume and produce message, and do kafka admin job",

	Run: func(cmd *cobra.Command, args []string) {
		print("healer here")
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringP("brokers", "b", "", "broker list, seperated by comma")
	rootCmd.PersistentFlags().StringP("client", "c", "", "client name")
	rootCmd.PersistentFlags().StringP("topic", "t", "", "topic name")

	rootCmd.AddCommand(getOffsetsCmd)

	flag.Parse()
	defer glog.Flush()
}

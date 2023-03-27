package cmd

import (
	"flag"
	"os"

	"github.com/golang/glog"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "healer",
	Short: "kafka tool. you can use it to consume and produce message, and do kafka admin job",

	Run: func(cmd *cobra.Command, args []string) {
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		glog.Error(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringP("brokers", "b", "", "broker list, seperated by comma")
	rootCmd.PersistentFlags().StringP("client", "c", "", "client name")
	rootCmd.PersistentFlags().StringP("topic", "t", "", "topic name")

	rootCmd.AddCommand(getOffsetsCmd)
	rootCmd.AddCommand(getMetadataCmd)
	rootCmd.AddCommand(simpleConsumerCmd)

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)

	flag.Parse()
	defer glog.Flush()
}

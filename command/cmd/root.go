package cmd

import (
	"flag"
	"os"

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
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringP("brokers", "b", "", "(required) broker list, seperated by comma")
	rootCmd.PersistentFlags().StringP("client", "c", "healer", "client name")

	rootCmd.AddCommand(getMetadataCmd)
	rootCmd.AddCommand(getOffsetsCmd)

	rootCmd.AddCommand(getPendingCmd)

	rootCmd.AddCommand(simpleConsumerCmd)
	rootCmd.AddCommand(simpleProducerCmd)
	rootCmd.AddCommand(consoleProducerCmd)
	rootCmd.AddCommand(consoleConsumerCmd)
	rootCmd.AddCommand(groupConsumerCmd)

	rootCmd.AddCommand(createTopicCmd)
	rootCmd.AddCommand(createPartitionsCmd)

	rootCmd.AddCommand(describeConfigsCmd)
	rootCmd.AddCommand(alterConfigsCmd)

	rootCmd.AddCommand(describeGroupsCmd)
	rootCmd.AddCommand(deleteGroupsCmd)

	rootCmd.AddCommand(listPartitionReassignmentsCmd)

	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)

	flag.Parse()
}

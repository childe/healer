package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer/client"
	"github.com/spf13/cobra"
)

var describeLogdirs = &cobra.Command{
	Use:   "describe-logdirs",
	Short: "describe logdirs of certain topics",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
		if err != nil {
			return err
		}
		clientID, err := cmd.Flags().GetString("client")
		if err != nil {
			return err
		}
		topics, err := cmd.Flags().GetStringSlice("topics")
		if err != nil {
			return err
		}

		admin, err := client.New(brokers, clientID)

		if err != nil {
			return err
		}

		responses, err := admin.DescribeLogDirs(topics)
		if err != nil {
			return err
		}

		s, err := json.MarshalIndent(responses, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(s))

		return nil
	},
}

func init() {
	describeLogdirs.Flags().StringSliceP("topics", "t", nil, "comma splited. A list of topics to list partition reassignments for (an empty list will return for all topics)")

	rootCmd.AddCommand(describeLogdirs)
}

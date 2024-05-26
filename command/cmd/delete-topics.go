package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var deleteTopicsCmd = &cobra.Command{
	Use:   "delete-topics",
	Short: "delete topics in kafka cluster",

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

		admin, err := healer.NewClient(brokers, clientID)

		if err != nil {
			return err
		}

		responses, err := admin.DeleteTopics(topics, 30000)
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
	deleteTopicsCmd.Flags().StringSlice("topics", nil, "topic names, separated by comma")

	rootCmd.AddCommand(deleteTopicsCmd)
}

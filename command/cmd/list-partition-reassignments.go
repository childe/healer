package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var listPartitionReassignmentsCmd = &cobra.Command{
	Use:   "list-partition-reassignments",
	Short: "list partition reassignments",

	RunE: func(cmd *cobra.Command, args []string) error {
		type tp struct {
			Topic     string `json:"topic"`
			Partition int32  `json:"partition"`
		}

		brokers, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")
		timeoutMS, err := cmd.Flags().GetInt32("timeout.ms")
		topicsJSONStr, err := cmd.Flags().GetString("topics")

		bs, err := healer.NewBrokers(brokers)

		if err != nil {
			return fmt.Errorf("could not create brokers from %s: %w", brokers, err)
		}

		req := healer.NewListPartitionReassignments(client, timeoutMS)
		if topicsJSONStr != "" {
			topics := make([]tp, 0)
			if err = json.Unmarshal([]byte(topicsJSONStr), &topics); err != nil {
				return fmt.Errorf("could not unmarshal topics: %w", err)
			}

			for _, topic := range topics {
				topicName := topic.Topic
				partitionID := topic.Partition

				req.AddTP(topicName, partitionID)
			}
		}
		resp, err := bs.ListPartitionReassignments(req)

		if err != nil {
			return fmt.Errorf("failed to do ListPartitionReassignments request: %w", err)
		}

		s, err := json.MarshalIndent(resp, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(s))

		return nil
	},
}

func init() {
	listPartitionReassignmentsCmd.Flags().Int32("timeout.ms", 30000, "The time in ms to wait for the request to complete")
	listPartitionReassignmentsCmd.Flags().StringP("topics", "t", "", `json format reassignments. {[{"topic":"test","partition":0}]`)
}

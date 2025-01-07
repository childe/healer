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
		type topicPartition struct {
			Topic     string `json:"topic"`
			Partition int32  `json:"partition"`
		}

		brokers, err := cmd.Flags().GetString("brokers")
		if err != nil {
			return err
		}
		client, err := cmd.Flags().GetString("client")
		if err != nil {
			return err
		}
		timeoutMS, err := cmd.Flags().GetInt32("timeout.ms")
		if err != nil {
			return err
		}

		config := healer.DefaultBrokerConfig()
		config.Net.TimeoutMSForEachAPI = make([]int, 68)
		config.Net.TimeoutMSForEachAPI[healer.API_ListPartitionReassignments] = int(timeoutMS)

		admin, err := healer.NewClient(brokers, client)
		if err != nil {
			return err
		}
		defer admin.Close()

		if err != nil {
			return fmt.Errorf("could not create brokers from %s: %w", brokers, err)
		}

		dataJSONStr, err := cmd.Flags().GetString("data")
		if err != nil {
			return err
		}

		req := healer.NewListPartitionReassignmentsRequest(client, timeoutMS)
		if dataJSONStr != "" {
			topicPartitions := make([]topicPartition, 0)
			err = json.Unmarshal([]byte(dataJSONStr), &topicPartitions)
			if err != nil {
				return err
			}

			for _, v := range topicPartitions {
				req.AddTP(v.Topic, v.Partition)
			}
		}

		resp, err := admin.ListPartitionReassignments(req)

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
	listPartitionReassignmentsCmd.Flags().StringP("data", "d", "", `json format data. [{"topic":"test","partition":0},{"topic":"test","partition":2}]`)
}

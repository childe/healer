package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var describeLogdirs = &cobra.Command{
	Use:   "describe-logdirs",
	Short: "describe logdirs of certain topics",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")
		topics, err := cmd.Flags().GetStringSlice("topics")

		bs, err := healer.NewBrokers(brokers)
		if err != nil {
			return fmt.Errorf("could not create brokers from %s: %w", brokers, err)
		}

		meta, err := bs.RequestMetaData(client, topics)
		if err != nil {
			return err
		}

		req := healer.NewDescribeLogDirsRequest(client, nil)

		for _, topic := range meta.TopicMetadatas {
			for _, partition := range topic.PartitionMetadatas {
				req.AddTopicPartition(topic.TopicName, partition.PartitionID)
			}
		}
		resp, err := bs.Request(req)

		if err != nil {
			return fmt.Errorf("failed to do DescribeLogdirs request: %w", err)
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
	describeLogdirs.Flags().StringSliceP("topics", "t", nil, "comma splited. A list of topics to list partition reassignments for (an empty list will return reassignments for all topics)")

	rootCmd.AddCommand(describeLogdirs)
}

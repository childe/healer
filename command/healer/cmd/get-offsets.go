package cmd

import (
	"fmt"
	"sort"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var getOffsetsCmd = &cobra.Command{
	Use:   "get-offsets",
	Short: "get offsets of a topic",

	RunE: func(cmd *cobra.Command, args []string) error {
		bs, err := cmd.Flags().GetString("brokers")
		if err != nil {
			return err
		}
		client, err := cmd.Flags().GetString("client")
		if err != nil {
			return err
		}
		topic, err := cmd.Flags().GetString("topic")
		if err != nil {
			return err
		}
		timestamp, err := cmd.Flags().GetInt64("timestamp")
		if err != nil {
			return err
		}
		if timestamp == -1 {
			//timestamp = time.Now().UnixMilli()
		} else if timestamp == -2 {
			timestamp = 0
		}

		brokers, err := healer.NewBrokers(bs)

		if err != nil {
			return fmt.Errorf("failed to create brokers from %s: %w", bs, err)
		}

		var offsets uint32 = 1
		offsetsResponse, err := brokers.RequestOffsets(client, topic, -1, timestamp, offsets)

		if err != nil {
			return fmt.Errorf("failed to get offsets: %w", err)
		}

		allPartitions := make([]healer.PartitionOffset, 0)

		for _, x := range offsetsResponse {
			for _, partitionOffsetsList := range x.TopicPartitionOffsets {
				allPartitions = append(allPartitions, partitionOffsetsList...)
			}
		}

		sort.Slice(allPartitions, func(i, j int) bool { return allPartitions[i].Partition < allPartitions[j].Partition })
		for _, p := range allPartitions {
			fmt.Printf("%s:%d:", topic, p.Partition)
			if len(p.OldStyleOffsets) > 0 {
				for i, offset := range p.OldStyleOffsets {
					if i != 0 {
						fmt.Print(",")
					}
					fmt.Printf("%d", offset)
				}
			} else {
				fmt.Printf("%d %d", p.Timestamp, p.Offset)
			}
			fmt.Println()
		}

		return nil
	},
}

func init() {
	getOffsetsCmd.Flags().Int64("timestamp", -1, "-1(latest)/-2(earliest). timestamp of the offsets before that")
	getOffsetsCmd.Flags().StringP("topic", "t", "", "topic name")
}

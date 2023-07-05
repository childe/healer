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
		client, err := cmd.Flags().GetString("client")
		topic, err := cmd.Flags().GetString("topic")
		offsets, err := cmd.Flags().GetUint32("offsets")
		timestamp, err := cmd.Flags().GetInt64("timestamp")

		brokers, err := healer.NewBrokers(bs)

		if err != nil {
			return fmt.Errorf("failed to create brokers from %s: %w", bs, err)
		}

		offsetsResponse, err := brokers.RequestOffsets(client, topic, -1, timestamp, offsets)

		if err != nil {
			return fmt.Errorf("failed to get offsets: %w", err)
		}

		rst := make([]healer.PartitionOffset, 0)

		for _, x := range offsetsResponse {
			for _, partitionOffsetsList := range x.TopicPartitionOffsets {
				rst = append(rst, partitionOffsetsList...)
			}
		}

		sort.Slice(rst, func(i, j int) bool { return rst[i].Partition < rst[j].Partition })
		for _, partitionOffset := range rst {
			fmt.Printf("%s:%d:", topic, partitionOffset.Partition)
			for i, offset := range partitionOffset.Offsets {
				if i != 0 {
					fmt.Print(",")
				}
				fmt.Printf("%d", offset)
			}
			fmt.Println()
		}

		return nil
	},
}

func init() {
	getOffsetsCmd.Flags().Uint32("offsets", 1, "get how many offsets")
	getOffsetsCmd.Flags().Int64("timestamp", -1, "-1(latest)/-2(earliest). timestamp of the offsets before that")
	getOffsetsCmd.Flags().StringP("topic", "t", "", "topic name")
}

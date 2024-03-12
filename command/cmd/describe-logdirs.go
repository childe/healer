package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
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

		leaderPartitions := make(map[int32]map[string]map[int32]struct{})
		for _, topic := range meta.TopicMetadatas {
			topicName := topic.TopicName
			for _, partition := range topic.PartitionMetadatas {
				for _, b := range partition.Replicas {
					pid := partition.PartitionID
					if _, ok := leaderPartitions[b]; !ok {
						leaderPartitions[b] = make(map[string]map[int32]struct{})
					}
					if _, ok := leaderPartitions[b][topic.TopicName]; !ok {
						leaderPartitions[b][topicName] = make(map[int32]struct{})
					}
					leaderPartitions[b][topicName][pid] = struct{}{}
				}
			}
		}

		rst := make(map[int32]healer.DescribeLogDirsResponse)
		for b, topicPartitions := range leaderPartitions {
			req := healer.NewDescribeLogDirsRequest(client, nil)
			for topicName, partitions := range topicPartitions {
				for pid := range partitions {
					req.AddTopicPartition(topicName, pid)
				}
			}

			broker, err := bs.GetBroker(b)
			if err != nil {
				return fmt.Errorf("failed to get broker %d: %w", b, err)
			}
			resp, err := broker.RequestAndGet(req)
			if err != nil {
				klog.Errorf("error from %s: %v", broker, err)
			}
			rst[b] = resp.(healer.DescribeLogDirsResponse)
		}

		s, err := json.MarshalIndent(rst, "", "  ")
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

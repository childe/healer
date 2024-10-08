package cmd

import (
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

var resetOffsetCmd = &cobra.Command{
	Use:   "reset-offset",
	Short: "reset offset of a group-topic",

	RunE: func(cmd *cobra.Command, args []string) error {
		bs, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")
		topic, err := cmd.Flags().GetString("topic")
		partitions, err := cmd.Flags().GetInt32Slice("partitions")
		group, err := cmd.Flags().GetString("group")
		offsetsStorage, err := cmd.Flags().GetString("offset.storage")
		timestamp, err := cmd.Flags().GetInt64("timestamp")

		userCustomPartitions := make(map[int32]struct{})
		for _, partition := range partitions {
			userCustomPartitions[partition] = struct{}{}
		}

		brokers, err := healer.NewBrokers(bs)

		if err != nil {
			return err
		}

		metaDataResponse, err := brokers.RequestMetaData(client, []string{topic})
		if err == nil {
			err = metaDataResponse.Error()
		}
		if err != nil {
			return fmt.Errorf("could not get metadata: %w", err)
		}

		offsetsResponses, err := brokers.RequestOffsets(client, topic, -1, timestamp, 1)
		if err != nil {
			return fmt.Errorf("request offsets error: %w. topic: %s, timestmap: %d", err, topic, timestamp)
		}

		offsets := make(map[int32]int64)
		for _, offsetsResponse := range offsetsResponses {
			if err := offsetsResponse.Error(); err != nil {
				return fmt.Errorf("request offsets error: %w. topic: %s, timestmap: %d", err, topic, timestamp)
			}
			for _, partitionOffsets := range offsetsResponse.TopicPartitionOffsets {
				for _, partitionOffset := range partitionOffsets {
					partition := partitionOffset.Partition
					offsets[partition] = partitionOffset.GetOffset()
				}
			}
		}

		// 1. get coordinator
		var coordinator *healer.Broker
		coordinatorResponse, err := brokers.FindCoordinator(client, group)
		if err != nil {
			return err
		}

		coordinator, err = brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
		if err != nil {
			return fmt.Errorf("could not get broker[%d]:%s", coordinatorResponse.Coordinator.NodeID, err)
		}
		klog.Infof("coordinator for group[%s]:%s", group, coordinator)

		// 4. commit
		var (
			apiVersion uint16
		)
		if offsetsStorage == "zookeeper" {
			apiVersion = 0
		} else {
			apiVersion = 2
		}
		offsetComimtReq := healer.NewOffsetCommitRequest(apiVersion, client, group)
		offsetComimtReq.SetMemberID("")
		offsetComimtReq.SetGenerationID(-1)
		offsetComimtReq.SetRetentionTime(-1)
		for partitionID, offset := range offsets {
			if len(userCustomPartitions) > 0 {
				if _, ok := userCustomPartitions[partitionID]; !ok {
					continue
				}
			}
			offsetComimtReq.AddPartiton(topic, partitionID, offset, "")
			klog.Infof("commit offset [%s][%d]:%d", topic, partitionID, offset)
		}

		_, err = coordinator.RequestAndGet(offsetComimtReq)
		if err != nil {
			return fmt.Errorf("commit offset error: %w", err)
		}
		return nil
	},
}

func init() {
	resetOffsetCmd.Flags().StringP("topic", "t", "", "topic name")
	resetOffsetCmd.Flags().Int32SliceP("partitions", "p", nil, "partitions. all partitions if not set")
	resetOffsetCmd.Flags().StringP("group", "g", "", "group name")
	resetOffsetCmd.Flags().Int64("timestamp", 0, "-2 to start offset, -1 to end offset")
	resetOffsetCmd.Flags().String("offsets.storage", "kafka", "kafka or zookeeper")
	resetOffsetCmd.MarkFlagRequired("topic")
	resetOffsetCmd.MarkFlagRequired("group")
	resetOffsetCmd.MarkFlagRequired("timestamp")
}

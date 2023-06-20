package cmd

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/childe/healer"
	"github.com/golang/glog"
	"github.com/spf13/cobra"
)

// getReplicas returns a list of brokers that used in 1 partition
func getReplicas(brokerIDs []int32, replicasCount int) (replicas []int32) {
	exited := map[int32]bool{}
	for len(replicas) < replicasCount {
		brokerID := brokerIDs[rand.Intn(len(brokerIDs))]
		if _, ok := exited[brokerID]; ok {
			continue
		}
		replicas = append(replicas, brokerID)
		exited[brokerID] = true
	}
	return
}

func genPartitionAssignmentsFromMeta(meta healer.MetadataResponse, count int) (partitionAssignments [][]int32, err error) {
	if len(meta.TopicMetadatas) != 1 {
		return nil, fmt.Errorf("topic not found")
	}
	needNewCount := count - len(meta.TopicMetadatas[0].PartitionMetadatas)
	if needNewCount <= 0 {
		return nil, fmt.Errorf("partition count %d is enough", count)
	}

	replicasCount := -1

	for _, partitionMetadata := range meta.TopicMetadatas[0].PartitionMetadatas {
		if replicasCount == -1 {
			replicasCount = len(partitionMetadata.Replicas)
		} else {
			if replicasCount != len(partitionMetadata.Replicas) {
				return nil, fmt.Errorf("replicas count not equal")
			}
		}
	}

	brokerIDs := []int32{}
	for _, broker := range meta.Brokers {
		brokerIDs = append(brokerIDs, broker.NodeID)
	}

	if len(brokerIDs) < replicasCount {
		return nil, fmt.Errorf("brokers count %d less than replicas %d", len(brokerIDs), replicasCount)
	}

	rand.Seed(time.Now().UnixNano())

	partitionAssignments = make([][]int32, needNewCount)
	for i := 0; i < needNewCount; i++ {
		partitionAssignments[i] = getReplicas(brokerIDs, replicasCount)
	}

	return
}
func genPartitionAssignments(assignments string) (partitionAssignments [][]int32, err error) {
	for i, brokerIDs := range strings.Split(assignments, ",") {
		if brokerIDs == "" {
			continue
		}
		partitionAssignments = append(partitionAssignments, []int32{})
		for _, brokerID := range strings.Split(brokerIDs, ":") {
			if _brokerID, err := strconv.Atoi(brokerID); err == nil {
				partitionAssignments[i] = append(partitionAssignments[i], int32(_brokerID))
			} else {
				return nil, fmt.Errorf("failed to convert %s to int: %w", brokerID, err)
			}
		}
	}
	return
}

var createPartitionsCmd = &cobra.Command{
	Use:   "create-partitions",
	Short: "create partitions",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")
		topic, err := cmd.Flags().GetString("topic")
		assignments, err := cmd.Flags().GetString("assignments")
		timeout, err := cmd.Flags().GetUint32("timeout")
		validateOnly, err := cmd.Flags().GetBool("validate-only")
		count, err := cmd.Flags().GetInt32("count")

		bs, err := healer.NewBrokers(brokers)
		if err != nil {
			return fmt.Errorf("failed to create brokers from %s", brokers)
		}

		controller, err := bs.GetBroker(bs.Controller())
		if err != nil {
			return fmt.Errorf("failed to create crotroller broker: %w", err)
		}

		r := healer.NewCreatePartitionsRequest(client, timeout, validateOnly)
		var partitionAssignments [][]int32
		if assignments != "" {
			partitionAssignments, err = genPartitionAssignments(assignments)
		} else {
			partitionAssignments = nil
		}
		if err != nil {
			return err
		}
		r.AddTopic(topic, count, partitionAssignments)

		if resp, err := controller.RequestAndGet(r); err == nil {
			b, _ := json.Marshal(resp.(healer.CreatePartitionsResponse))
			glog.Info(string(b))
		} else {
			return fmt.Errorf("failed to create partitions: %w", err)
		}
		return nil
	},
}

func init() {
	createPartitionsCmd.Flags().StringP("topic", "t", "", "The topic name.")
	createPartitionsCmd.Flags().String("assignments", "", "The new partition assignments. broker_id_for_part1_replica1:broker_id_for_part1_replica2,broker_id_for_part2_replica1:broker_id_for_part2_replica2,... ")
	createPartitionsCmd.Flags().Int32("count", 0, "The new partition count.")
	createPartitionsCmd.Flags().Uint32("timeout", 30000, "The time in ms to wait for the partitions to be created.")
	createPartitionsCmd.Flags().Bool("validate-only", true, "If true, then validate the request, but don't actually increase the number of partitions.")
}

package cmd

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var createTopicCmd = &cobra.Command{
	Use:   "createtopic",
	Short: "create topic",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")
		topic, err := cmd.Flags().GetString("topic")
		partitions, err := cmd.Flags().GetInt32("partitions")
		replicationFactor, err := cmd.Flags().GetInt16("replication-factor")
		replicaAssignment, err := cmd.Flags().GetString("replica-assignment")
		timeout, err := cmd.Flags().GetUint32("timeout")

		if partitions != -1 && replicaAssignment != "" {
			return fmt.Errorf(`Option "[partitions]" can't be used with option"[partitions]"`)
		}

		if replicationFactor != -1 && replicaAssignment != "" {
			return fmt.Errorf(`Option "[replica-assignment]" can't be used with option"[replication-factor]"`)
		}

		bs, err := healer.NewBrokers(brokers)
		if err != nil {
			return fmt.Errorf("failed to create brokers from %s", brokers)
		}

		controller, err := bs.GetBroker(bs.Controller())
		if err != nil {
			return fmt.Errorf("failed to create crotroller broker: %w", err)
		}

		r := healer.NewCreateTopicsRequest(client, timeout)

		replicas := make(map[int32][]int32)
		if replicaAssignment != "" {
			for pid, nodes := range strings.Split(replicaAssignment, ",") {
				replicas[int32(pid)] = make([]int32, 0)
				for _, node := range strings.Split(nodes, ":") {
					nodeid, err := strconv.Atoi(node)
					if err != nil {
						return fmt.Errorf("invalid replica-assignment: %w", err)
					}
					replicas[int32(pid)] = append(replicas[int32(pid)], int32(nodeid))
				}
			}

			for pid, nodes := range replicas {
				r.AddReplicaAssignment(topic, pid, nodes)
			}
		} else {
			r.AddTopic(topic, partitions, replicationFactor)
		}

		if _, err = controller.Request(r); err != nil {
			return fmt.Errorf("faild to request create-topic: %w", err)
		}
		return nil
	},
}

func init() {
	createTopicCmd.Flags().Int32("partitions", -1, "The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions")
	createTopicCmd.Flags().Int16("replication-factor", -1, "The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor")
	createTopicCmd.Flags().String("replica-assignment", "", "pid:[replicas],pid:[replicas]...")
	createTopicCmd.Flags().Uint32("timeout", 30000, "How long to wait in milliseconds before timing out the request")
}

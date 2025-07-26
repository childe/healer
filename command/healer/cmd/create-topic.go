package cmd

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

var createTopicCmd = &cobra.Command{
	Use:   "create-topic",
	Short: "create topic",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
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
		partitions, err := cmd.Flags().GetInt32("partitions")
		if err != nil {
			return err
		}
		replicationFactor, err := cmd.Flags().GetInt16("replication-factor")
		if err != nil {
			return err
		}
		replicaAssignment, err := cmd.Flags().GetString("replica-assignment")
		if err != nil {
			return err
		}
		timeout, err := cmd.Flags().GetUint32("timeout")
		if err != nil {
			return err
		}

		if partitions != -1 && replicaAssignment != "" {
			return fmt.Errorf(`option "[partitions]" can't be used with option"[replica-assignment]"`)
		}

		if replicationFactor != -1 && replicaAssignment != "" {
			return fmt.Errorf(`option "[replica-assignment]" can't be used with option"[replication-factor]"`)
		}

		// Use the new Client.CreateTopic method for simple cases (no replica assignment)
		if replicaAssignment == "" {
			// Create client instance
			healerClient, err := healer.NewClient(brokers, client)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}
			defer healerClient.Close()

			// Use the new CreateTopic method
			resp, err := healerClient.CreateTopic(topic, partitions, replicationFactor, timeout)
			if err != nil {
				return fmt.Errorf("failed to create topic: %w", err)
			}

			// Print response
			b, _ := json.Marshal(resp)
			klog.Info(string(b))
			return nil
		}

		// For complex cases with replica assignment, use the original implementation
		config := healer.DefaultBrokerConfig()
		config.Net.TimeoutMSForEachAPI = make([]int, 68)
		config.Net.TimeoutMSForEachAPI[healer.API_CreateTopics] = int(timeout)
		bs, err := healer.NewBrokersWithConfig(brokers, config)
		if err != nil {
			return fmt.Errorf("failed to create brokers from %s", brokers)
		}

		controller, err := bs.GetBroker(bs.Controller())
		if err != nil {
			return fmt.Errorf("failed to create crotroller broker: %w", err)
		}

		r := healer.NewCreateTopicsRequest(client, timeout)

		replicas := make(map[int32][]int32)
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

		// First add the topic
		r.AddTopic(topic, partitions, replicationFactor)

		// Then add replica assignments
		for pid, nodes := range replicas {
			r.AddReplicaAssignment(topic, pid, nodes)
		}

		if resp, err := controller.RequestAndGet(r); err != nil {
			return fmt.Errorf("failed to create topics: %w", err)
		} else {
			b, _ := json.Marshal(resp.(healer.CreateTopicsResponse))
			klog.Info(string(b))
		}
		return nil
	},
}

func init() {
	createTopicCmd.Flags().Int32("partitions", -1, "The number of partitions to create in the topic, or -1 if we are either specifying a manual partition assignment or using the default partitions")
	createTopicCmd.Flags().Int16("replication-factor", -1, "The number of replicas to create for each partition in the topic, or -1 if we are either specifying a manual partition assignment or using the default replication factor")
	createTopicCmd.Flags().String("replica-assignment", "", "pid:[replicas],pid:[replicas]...")
	createTopicCmd.Flags().Uint32("timeout", 30000, "How long to wait in milliseconds before timing out the request")
	createTopicCmd.Flags().StringP("topic", "t", "", "topic name")
}

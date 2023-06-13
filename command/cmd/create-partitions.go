package cmd

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/childe/healer"
	"github.com/golang/glog"
	"github.com/spf13/cobra"
)

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
		glog.Infof("brokers: %s, client: %s, topic: %s, assignments: %v, timeout: %d, validate-only: %v", brokers, client, topic, assignments, timeout, validateOnly)

		bs, err := healer.NewBrokers(brokers)
		if err != nil {
			return fmt.Errorf("failed to create brokers from %s", brokers)
		}

		controller, err := bs.GetBroker(bs.Controller())
		if err != nil {
			return fmt.Errorf("failed to create crotroller broker: %w", err)
		}

		r := healer.NewCreatePartitionsRequest(client, timeout, validateOnly)
		partitionAssignments := [][]int32{}
		if assignments != "" {
			for i, brokerIDs := range strings.Split(assignments, ",") {
				if brokerIDs == "" {
					continue
				}
				partitionAssignments = append(partitionAssignments, []int32{})
				for _, brokerID := range strings.Split(brokerIDs, ":") {
					if _brokerID, err := strconv.Atoi(brokerID); err == nil {
						partitionAssignments[i] = append(partitionAssignments[i], int32(_brokerID))
					} else {
						return fmt.Errorf("failed to convert %s to int: %w", brokerID, err)
					}
				}
			}
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
	createPartitionsCmd.Flags().String("assignments", "", "The new partition assignments.")
	createPartitionsCmd.Flags().Int32("count", 0, "The new partition count.")
	createPartitionsCmd.Flags().Uint32("timeout", 30000, "The time in ms to wait for the partitions to be created.")
	createPartitionsCmd.Flags().Bool("validate-only", true, "If true, then validate the request, but don't actually increase the number of partitions.")
}

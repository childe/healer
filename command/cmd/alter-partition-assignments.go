package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/golang/glog"
	"github.com/spf13/cobra"
)

var alterPartitionReassignmentsCmd = &cobra.Command{
	Use:   "alter-partition-reassignments",
	Short: "alter partition reassignments",

	RunE: func(cmd *cobra.Command, args []string) error {
		type reassignment struct {
			Topic     string  `json:"topic"`
			Partition int32   `json:"partition"`
			Replicas  []int32 `json:"replicas"`
		}
		reassignmentsJSONStr, err := cmd.Flags().GetString("reassignments")
		if err != nil {
			return err
		}
		reassignments := make([]reassignment, 0)
		err = json.Unmarshal([]byte(reassignmentsJSONStr), &reassignments)
		if err != nil {
			return err
		}

		brokers, err := cmd.Flags().GetString("brokers")
		timeoutMS, err := cmd.Flags().GetInt32("timeout.ms")
		config := healer.DefaultBrokerConfig()
		config.NetConfig.TimeoutMSForEachAPI = make([]int, 68)
		config.NetConfig.TimeoutMSForEachAPI[healer.API_AlterPartitionReassignments] = int(timeoutMS)
		bs, err := healer.NewBrokersWithConfig(brokers, config)
		if err != nil {
			return fmt.Errorf("failed to create brokers from %s", brokers)
		}

		req := healer.NewAlterPartitionReassignmentsRequest(timeoutMS)
		for _, v := range reassignments {
			req.AddAssignment(v.Topic, v.Partition, v.Replicas)
		}
		resp, err := bs.AlterPartitionReassignments(&req)
		if err != nil {
			return err
		}

		b, _ := json.MarshalIndent(resp, "", "  ")
		glog.Info(string(b))

		return nil
	},
}

func init() {
	alterPartitionReassignmentsCmd.Flags().StringP("reassignments", "r", "", `json format reassignments. [{"topic":"test","partition":0,"replicas":[1,2,3]}]`)
	alterPartitionReassignmentsCmd.Flags().Int32("timeout.ms", 30000, "timeout in ms")
	rootCmd.AddCommand(alterPartitionReassignmentsCmd)
}

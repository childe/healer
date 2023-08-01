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

		brokers, err := cmd.Flags().GetString("brokers")
		// client, err := cmd.Flags().GetString("client")

		reassignmentsJSONStr, err := cmd.Flags().GetString("reassignments")
		if err != nil {
			return err
		}

		reassignments := make([]reassignment, 0)
		err = json.Unmarshal([]byte(reassignmentsJSONStr), &reassignments)
		if err != nil {
			return err
		}

		timeout, err := cmd.Flags().GetInt32("timeout")
		if err != nil {
			return err
		}

		bs, err := healer.NewBrokers(brokers)
		if err != nil {
			return fmt.Errorf("failed to create brokers from %s", brokers)
		}

		req := healer.NewAlterPartitionReassignmentsRequest(timeout)
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
	alterPartitionReassignmentsCmd.Flags().StringP("reassignments", "r", "", `json format reassignments. {[{"topic":"test","partition":0,"replicas":[1,2,3]}]`)
	alterPartitionReassignmentsCmd.Flags().Int32("timeout", 30000, "timeout in ms")
	rootCmd.AddCommand(alterPartitionReassignmentsCmd)
}

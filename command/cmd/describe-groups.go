package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/golang/glog"
	"github.com/spf13/cobra"
)

var describeGroupsCmd = &cobra.Command{
	Use:   "describe-groups",
	Short: "describe groups in kafka cluster",

	RunE: func(cmd *cobra.Command, args []string) error {
		brorkers, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")
		group, err := cmd.Flags().GetString("group")

		brokers, err := healer.NewBrokers(brorkers)
		if err != nil {
			return fmt.Errorf("failed to get offsets: %w", err)
		}

		coordinatorResponse, err := brokers.FindCoordinator(client, group)
		if err != nil {
			return err
		}

		coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
		if err != nil {
			return err
		}

		glog.Infof("coordinator for group[%s]:%s", group, coordinator.GetAddress())

		req := healer.NewDescribeGroupsRequest(client, []string{group})
		respBody, err := coordinator.Request(req)
		if err != nil {
			return fmt.Errorf("failed to request describe_groups: %w", err)
		}
		resp, err := healer.NewDescribeGroupsResponse(respBody)

		if err != nil {
			return fmt.Errorf("failed to get describe_groups response: %w", err)
		}

		s, err := json.MarshalIndent(resp, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal metadata response: %w", err)
		}
		fmt.Println(string(s))

		return nil
	},
}

func init() {
	describeGroupsCmd.Flags().String("group", "", "group names")
}

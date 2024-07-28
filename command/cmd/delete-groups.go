package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
	"k8s.io/klog/v2"
)

var deleteGroupsCmd = &cobra.Command{
	Use:   "delete-groups",
	Short: "delete groups in kafka cluster",

	RunE: func(cmd *cobra.Command, args []string) error {
		bootStrapBrokers, err := cmd.Flags().GetString("brokers")
		if err != nil {
			return err
		}
		client, err := cmd.Flags().GetString("client")
		if err != nil {
			return err
		}
		groups, err := cmd.Flags().GetStringSlice("groups")
		if err != nil {
			return err
		}

		brokers, err := healer.NewBrokers(bootStrapBrokers)
		if err != nil {
			return err
		}

		for _, group := range groups {
			coordinatorResponse, err := brokers.FindCoordinator(client, group)
			if err != nil {
				return err
			}

			coordinator, err := brokers.GetBroker(coordinatorResponse.Coordinator.NodeID)
			if err != nil {
				return err
			}
			klog.Infof("coordinator for group[%s]:%s", group, coordinator.GetAddress())

			req := healer.NewDeleteGroupsRequest(client, groups)
			resp, err := coordinator.RequestAndGet(req)
			if err != nil {
				return fmt.Errorf("failed to request delete_groups: %w", err)
			}

			s, err := json.MarshalIndent(resp.(healer.DeleteGroupsResponse), "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal delete_groups response: %w", err)
			}
			fmt.Println(string(s))
		}

		return nil
	},
}

func init() {
	deleteGroupsCmd.Flags().StringSlice("groups", nil, "group names, separated by comma")
}

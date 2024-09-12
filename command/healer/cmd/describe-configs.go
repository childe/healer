package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var describeConfigsCmd = &cobra.Command{
	Use:   "describe-configs",
	Short: "describe configs",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")
		resourceType, err := cmd.Flags().GetString("resource-type")
		resourceName, err := cmd.Flags().GetString("resource-name")
		keys, err := cmd.Flags().GetStringArray("keys")
		all, err := cmd.Flags().GetBool("all")

		bs, err := healer.NewBrokers(brokers)
		if err != nil {
			return fmt.Errorf("failed to create brokers from %s", brokers)
		}

		controller, err := bs.GetBroker(bs.Controller())
		if err != nil {
			return fmt.Errorf("failed to create crotroller broker: %w", err)
		}

		if all {
			keys = nil
		}
		resources := []*healer.DescribeConfigsRequestResource{
			{
				ResourceType: healer.ConvertConfigResourceType(resourceType),
				ResourceName: resourceName,
				ConfigNames:  keys,
			},
		}
		r := healer.NewDescribeConfigsRequest(client, resources)

		resp, err := controller.RequestAndGet(r)
		if err != nil {
			return fmt.Errorf("faild to make describe-configs request: %w", err)
		}

		b, _ := json.MarshalIndent(resp.(healer.DescribeConfigsResponse), "", "  ")
		fmt.Println(string(b))
		return nil
	},
}

func init() {
	describeConfigsCmd.Flags().String("resource-type", "", "the resource type: topic, broker, broker_logger")
	describeConfigsCmd.Flags().String("resource-name", "", "the resource name")
	describeConfigsCmd.Flags().StringArray("keys", []string{}, "the configuration keys to list, or null to list all configuration keys")
	describeConfigsCmd.Flags().Bool("all", false, "list all configs for the given entity")
	describeConfigsCmd.MarkFlagsMutuallyExclusive("keys", "all")
}

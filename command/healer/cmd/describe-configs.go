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
		if err != nil {
			return err
		}
		client, err := cmd.Flags().GetString("client")
		if err != nil {
			return err
		}
		resourceType, err := cmd.Flags().GetString("resource-type")
		if err != nil {
			return err
		}
		resourceName, err := cmd.Flags().GetString("resource-name")
		if err != nil {
			return err
		}

		var keys []string
		if !cmd.Flags().Changed("keys") {
			keys = nil
		} else {
			if keys, err = cmd.Flags().GetStringArray("keys"); err != nil {
				return err
			}
		}

		admin, err := healer.NewClient(brokers, client)
		if err != nil {
			return err
		}

		resp, err := admin.DescribeConfigs(resourceType, resourceName, keys)
		if err != nil {
			return err
		}

		b, _ := json.MarshalIndent(resp, "", "  ")
		fmt.Println(string(b))
		return nil
	},
}

func init() {
	describeConfigsCmd.Flags().String("resource-type", "", "the resource type: topic, broker, broker_logger")
	describeConfigsCmd.Flags().String("resource-name", "", "the resource name")
	describeConfigsCmd.Flags().StringArray("keys", []string{},
		"the configuration keys to list, or don't set this to list all")
}

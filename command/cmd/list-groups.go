package cmd

import (
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var listGroupsCmd = &cobra.Command{
	Use:   "list-groups",
	Short: "list groups in kafka cluster",

	RunE: func(cmd *cobra.Command, args []string) error {
		bs, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")

		helper, err := healer.NewHelper(bs, client, healer.DefaultBrokerConfig())
		if err != nil {
			return err
		}

		for _, g := range helper.GetGroups() {
			fmt.Println(g)
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(listGroupsCmd)
}

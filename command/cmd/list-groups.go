package cmd

import (
	"fmt"

	"github.com/childe/healer/client"
	"github.com/spf13/cobra"
)

var listGroupsCmd = &cobra.Command{
	Use:   "list-groups",
	Short: "list groups in kafka cluster",

	RunE: func(cmd *cobra.Command, args []string) error {
		bs, err := cmd.Flags().GetString("brokers")
		if err != nil {
			return err
		}
		clientID, err := cmd.Flags().GetString("client")
		if err != nil {
			return err
		}
		client, err := client.New(bs, clientID)
		if err != nil {
			return err
		}
		groups, err := client.ListGroups()
		if err != nil {
			return err
		}

		for _, g := range groups {
			fmt.Println(g)
		}

		return err
	},
}

func init() {
	rootCmd.AddCommand(listGroupsCmd)
}

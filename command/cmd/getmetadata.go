/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var getMetadataCmd = &cobra.Command{
	Use:   "getoffsets",
	Short: "get offsets of a topic",

	RunE: func(cmd *cobra.Command, args []string) error {
		brorkers, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")
		topic, err := cmd.Flags().GetString("topic")

		brokers, err := healer.NewBrokers(brorkers)

		if err != nil {
			return fmt.Errorf("failed to get offsets: %w", err)
		}

		var metadataResponse *healer.MetadataResponse
		if topic == "" {
			metadataResponse, err = brokers.RequestMetaData(client, nil)
		} else {
			metadataResponse, err = brokers.RequestMetaData(client, []string{topic})
		}

		if err != nil {
			return fmt.Errorf("failed to get metadata response: %w", err)
		}

		s, err := json.MarshalIndent(metadataResponse, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal metadata response: %w", err)
		}

		fmt.Println(string(s))

		return nil
	},
}

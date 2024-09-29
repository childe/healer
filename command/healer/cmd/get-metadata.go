package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var getMetadataCmd = &cobra.Command{
	Use:   "get-metadata",
	Short: "get metadata of a cluster. return all topics if topic is not specified",

	RunE: func(cmd *cobra.Command, args []string) error {
		bs, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")
		topics, err := cmd.Flags().GetStringSlice("topics")
		format, err := cmd.Flags().GetString("format")

		brokers, err := healer.NewBrokers(bs)

		if err != nil {
			return fmt.Errorf("failed to get offsets: %w", err)
		}

		var metadataResponse healer.MetadataResponse
		if len(topics) == 0 {
			metadataResponse, err = brokers.RequestMetaData(client, nil)
		} else {
			metadataResponse, err = brokers.RequestMetaData(client, topics)
		}

		switch format {
		case "json":
			s, err := json.MarshalIndent(metadataResponse, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal metadata response: %w", err)
			}
			fmt.Println(string(s))
		case "cat":
			catResponse(&metadataResponse)
		}

		return nil
	},
}

func catResponse(metadataResponse *healer.MetadataResponse) {
	fmt.Println("brokers:")
	bs := make(map[int32]string)
	for _, b := range metadataResponse.Brokers {
		address := b.NetAddress()
		fmt.Printf("%d %s\n", b.NodeID, address)
		bs[b.NodeID] = address
	}

	fmt.Println("topics:")
	for _, t := range metadataResponse.TopicMetadatas {
		topicName := t.TopicName
		for _, p := range t.PartitionMetadatas {
			broker := bs[p.Leader]
			fmt.Printf("%s %d %s\n", topicName, p.PartitionID, broker)
		}
	}
}

func init() {
	getMetadataCmd.Flags().String("format", "json", "default is json, support json, cat")
	getMetadataCmd.Flags().StringSliceP("topics", "t", nil, "topics, comma separated")
}

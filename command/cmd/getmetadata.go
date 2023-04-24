package cmd

import (
	"encoding/json"
	"fmt"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var getMetadataCmd = &cobra.Command{
	Use:   "getmetadata",
	Short: "get metadata of a cluster. return all topics if topic is not specified",

	RunE: func(cmd *cobra.Command, args []string) error {
		brorkers, err := cmd.Flags().GetString("brokers")
		client, err := cmd.Flags().GetString("client")
		topic, err := cmd.Flags().GetString("topic")
		format, err := cmd.Flags().GetString("format")

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

		switch format {
		case "json":
			s, err := json.MarshalIndent(metadataResponse, "", "  ")
			if err != nil {
				return fmt.Errorf("failed to marshal metadata response: %w", err)
			}
			fmt.Println(string(s))
		case "cat":
			catResponse(metadataResponse)
		}

		return nil
	},
}

func catResponse(metadataResponse *healer.MetadataResponse) {
	fmt.Println("brokers:")
	bs := make(map[int32]string)
	for _, b := range metadataResponse.Brokers {
		fmt.Printf("%d %s:%d\n", b.NodeID, b.Host, b.Port)
		bs[b.NodeID] = fmt.Sprintf("%s:%d", b.Host, b.Port)
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
}

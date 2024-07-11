package cmd

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var simpleProducerCmd = &cobra.Command{
	Use:   "simple-producer",
	Short: "produce message to a certain partition of a topic",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
		if err != nil {
			return err
		}
		producerConfig := map[string]interface{}{"bootstrap.servers": brokers}
		client, err := cmd.Flags().GetString("client")
		if err != nil {
			return err
		}
		if client != "" {
			producerConfig["client.id"] = client
		}
		partition, err := cmd.Flags().GetInt32("partition")
		if err != nil {
			return err
		}
		topic, err := cmd.Flags().GetString("topic")
		if err != nil {
			return err
		}
		if topic == "" || err != nil {
			return errors.New("topic must be specified")
		}

		config, err := cmd.Flags().GetString("config")
		if err != nil {
			return err
		}
		json.Unmarshal([]byte(config), &producerConfig)

		simpleProducer, err := healer.NewSimpleProducer(context.Background(), topic, partition, producerConfig)
		if err != nil {
			return err
		}
		defer simpleProducer.Close()

		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := scanner.Bytes()
			if err = simpleProducer.AddMessage(nil, line); err != nil {
				return fmt.Errorf("add message error: %w", err)
			}
		}

		if err := scanner.Err(); err != nil {
			return fmt.Errorf("scan stdin error: %w", err)
		}

		return nil
	},
}

func init() {
	simpleProducerCmd.Flags().String("config", "", `{"xx"="yy","aa"="zz"} refer to https://github.com/childe/healer/blob/master/config.go`)
	simpleProducerCmd.Flags().Int32("partition", 0, "partition id")
	simpleProducerCmd.Flags().StringP("topic", "t", "", "topic name")
}

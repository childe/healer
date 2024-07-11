package cmd

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var consoleProducerCmd = &cobra.Command{
	Use:   "console-producer",
	Short: "produce message to a certain topic",

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

		consoleProducer, err := healer.NewProducer(topic, producerConfig)
		if err != nil {
			return fmt.Errorf("could not create ConsoleProducer: %w", err)
		}

		defer consoleProducer.Close()

		scanner := bufio.NewScanner(os.Stdin)
		for scanner.Scan() {
			line := scanner.Bytes()
			if err = consoleProducer.AddMessage(nil, line); err != nil {
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
	consoleProducerCmd.Flags().String("config", "", `{"xx"="yy","aa"="zz"} refer to https://github.com/childe/healer/blob/master/config.go`)
	consoleProducerCmd.Flags().StringP("topic", "t", "", "topic name")
}

package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/childe/healer"
	"github.com/spf13/cobra"
)

var consoleProducerCmd = &cobra.Command{
	Use:   "console-producer",
	Short: "produce message to a certain topic",

	RunE: func(cmd *cobra.Command, args []string) error {
		brokers, err := cmd.Flags().GetString("brokers")
		producerConfig := map[string]interface{}{"bootstrap.servers": brokers}
		client, err := cmd.Flags().GetString("client")
		if client != "" {
			producerConfig["client.id"] = client
		}
		topic, err := cmd.Flags().GetString("topic")
		if topic == "" || err != nil {
			return errors.New("topic must be specified")
		}

		config, err := cmd.Flags().GetString("config")

		for _, kv := range strings.Split(config, ",") {
			if strings.Trim(kv, " ") == "" {
				continue
			}
			t := strings.SplitN(kv, "=", 2)
			if len(t) != 2 {
				return fmt.Errorf("invalid config : %s", kv)
			}
			producerConfig[t[0]] = t[1]
		}

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
	consoleProducerCmd.Flags().String("config", "", "XX=YY,AA=ZZ. refer to https://github.com/childe/healer/blob/master/config.go")
	consoleProducerCmd.Flags().StringP("topic", "t", "", "topic name")
}

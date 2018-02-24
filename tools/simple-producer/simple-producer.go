package main

import (
	"flag"
	"os"

	"github.com/childe/healer"
)

var (
	brokers   = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to.")
	topic     = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	partition = flag.Int("partition", 0, "The partition to consume from.")
	value     = flag.String("value", "", "")
)

func main() {
	flag.Parse()

	if *topic == "" {
		flag.PrintDefaults()
		os.Exit(4)
	}
	if *value == "" {
		flag.PrintDefaults()
		os.Exit(4)
	}

	simpleProducer := &healer.SimpleProducer{}
	simpleProducer.Config.ClientId = "console-producer"
	simpleProducer.Config.Broker = *brokers
	simpleProducer.Config.TopicName = *topic
	simpleProducer.Config.Partition = int32(*partition)
	simpleProducer.Config.RequiredAcks = 0
	simpleProducer.Config.Timeout = 0
	simpleProducer.Config.MessageCap = 1
	simpleProducer.MessageSet = make([]healer.Message, 1)
	simpleProducer.MessageSetSize = 0

	message := healer.Message{
		MagicByte:  0,
		Attributes: 0,
		Key:        nil,
		Value:      []byte(*value),
	}
	simpleProducer.AddMessage(message)
}

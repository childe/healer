package main

import (
	"flag"
	"fmt"
	"github.com/childe/gokafka"
	"log"
	"math"
	"os"
)

var (
	brokers     = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to.")
	topic       = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	partition   = flag.Int("partition", 0, "The partition to consume from.")
	offset      = flag.Int64("offset", 0, "The offset id to consume from, default to -2 which means from beginning; while value -1 means from end.")
	clientId    = flag.String("clientId", "gokafkaShell", "The ID of this client.")
	minBytes    = flag.Int("min-bytes", 1, "The fetch size of each request.")
	maxWaitTime = flag.Int("max-wait-ms", 10000, "The max amount of time each fetch request waits.")
	maxBytes    = flag.Int("max-bytes", math.MaxInt32, "The maximum bytes to include in the message set for this partition. This helps bound the size of the response.")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *topic == "" {
		fmt.Println("need topic!")
		flag.PrintDefaults()
	}

	simpleConsumer := &gokafka.SimpleConsumer{}
	simpleConsumer.ClientId = *clientId
	simpleConsumer.Brokers = *brokers
	simpleConsumer.TopicName = *topic
	simpleConsumer.Partition = int32(*partition)
	simpleConsumer.FetchOffset = *offset
	simpleConsumer.MaxWaitTime = int32(*maxWaitTime)
	simpleConsumer.MaxBytes = int32(*maxBytes)
	simpleConsumer.MinBytes = int32(*minBytes)

	messages := make(chan gokafka.Message)
	go func() { simpleConsumer.Consume(messages) }()
	for {
		message := <-messages
		log.Println(string(message.Value))
	}
}

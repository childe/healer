package main

//TODO maxBytes

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"

	"github.com/childe/gokafka"
)

var (
	brokers     = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to(defautl: 127.0.0.1:9092).")
	topic       = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	partition   = flag.Int("partition", 0, "The partition to consume from.")
	offset      = flag.Int64("offset", -2, "The offset id to consume from, default to -2 which means from beginning; while value -1 means from end(default -2).")
	clientID    = flag.String("clientID", "gokafka", "The ID of this client.")
	minBytes    = flag.Int("min-bytes", 1, "The fetch size of each request.")
	maxWaitTime = flag.Int("max-wait-ms", 10000, "The max amount of time(ms) each fetch request waits(default 10000).")
	maxMessages = flag.Int("max-messages", math.MaxInt32, "The number of messages to consume (default: 2147483647)")
	// maxBytes    = flag.Int("max-bytes", math.MaxInt32, "The maximum bytes to include in the message set for this partition. This helps bound the size of the response.")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *topic == "" {
		fmt.Println("need topic!")
		flag.PrintDefaults()
	}

	simpleConsumer := &gokafka.SimpleConsumer{}
	simpleConsumer.ClientID = *clientID
	simpleConsumer.Brokers = *brokers
	simpleConsumer.TopicName = *topic
	simpleConsumer.Partition = int32(*partition)
	simpleConsumer.FetchOffset = *offset
	simpleConsumer.MaxWaitTime = int32(*maxWaitTime)
	simpleConsumer.MaxBytes = math.MaxInt32
	simpleConsumer.MinBytes = int32(*minBytes)

	i := 0
	messages := make(chan gokafka.Message)
	go func() { simpleConsumer.Consume(messages) }()
	for {
		message := <-messages
		fmt.Printf("%d: %s\n", message.Offset, message.Value)
		i++
		if i >= *maxMessages {
			os.Exit(0)
		}
	}
}

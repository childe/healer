package main

//TODO maxBytes

import (
	"flag"
	"fmt"
	"math"
	"os"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	brokers        = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to(defautl: 127.0.0.1:9092).")
	topic          = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	partition      = flag.Int("partition", 0, "The partition to consume from.")
	offset         = flag.Int64("offset", -2, "The offset id to consume from, default to -2 which means from beginning; while value -1 means from end(default -2).")
	clientID       = flag.String("clientID", "healer", "The ID of this client.")
	minBytes       = flag.Int("min-bytes", 1, "The fetch size of each request.")
	maxWaitTime    = flag.Int("max-wait-ms", 10000, "The max amount of time(ms) each fetch request waits(default 10000).")
	maxMessages    = flag.Int("max-messages", math.MaxInt32, "The number of messages to consume (default: 2147483647)")
	maxBytes       = flag.Int("max-bytes", math.MaxInt32, "The maximum bytes to include in the message set for this partition. This helps bound the size of the response.")
	connectTimeout = flag.Int("connect-timeout", 10, "default 10 Second. connect timeout to broker")
	timeout        = flag.Int("timeout", 30, "default 30 Second. read timeout from connection to broker")
)

func main() {
	flag.Parse()

	if *topic == "" {
		flag.PrintDefaults()
		fmt.Println("need topic name")
		os.Exit(4)
	}

	var err error
	simpleConsumer := &healer.SimpleConsumer{}
	simpleConsumer.ClientID = *clientID
	simpleConsumer.Brokers, err = healer.NewBrokers(*brokers, *clientID, *connectTimeout, *timeout)
	if err != nil {
		glog.Fatalf("could not init brokers from %s:%s", *brokers, err)
	}
	simpleConsumer.TopicName = *topic
	simpleConsumer.Partition = int32(*partition)
	simpleConsumer.FetchOffset = *offset
	simpleConsumer.MaxWaitTime = int32(*maxWaitTime)
	simpleConsumer.MaxBytes = int32(*maxBytes)
	simpleConsumer.MinBytes = int32(*minBytes)

	i := 0
	messages := make(chan *healer.Message)
	go func() { simpleConsumer.ConsumeStreamingly(messages, *maxMessages) }()
	for {
		message := <-messages
		fmt.Printf("%d: %s\n", message.Offset, message.Value)
		i++
		if i >= *maxMessages {
			os.Exit(0)
		}
	}
}

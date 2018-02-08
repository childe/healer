package main

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
	clientID       = flag.String("clientID", "", "The ID of this client.")
	groupID        = flag.String("groupID", "", "REQUIRED: The ID of this client.")
	minBytes       = flag.Int("min-bytes", 1, "The fetch size of each request.")
	fromBeginning  = flag.Bool("from-beginning", false, "default false")
	maxWaitTime    = flag.Int("max-wait-ms", 10000, "The max amount of time(ms) each fetch request waits(default 10000).")
	maxMessages    = flag.Int("max-messages", math.MaxInt32, "The number of messages to consume (default: 2147483647)")
	maxBytes       = flag.Int("max-bytes", 10*1024*1024, "The maximum bytes to include in the message set for this partition. This helps bound the size of the response.")
	connectTimeout = flag.Int("connect-timeout", 30, "default 30 Second. connect timeout to broker")
	offsetsStorage = flag.String("offsets.storage", "kafka", "default kafka. Select where offsets should be stored (zookeeper or kafka).")
	timeout        = flag.Int("timeout", 40, "default 10 Second. read timeout from connection to broker")
	sessionTimeout = flag.Int("sessionTimeout", 30000, "default 30000ms. The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms.")
)

func main() {
	flag.Parse()

	if *topic == "" {
		flag.PrintDefaults()
		fmt.Println("need topic name")
		os.Exit(4)
	}

	if *groupID == "" {
		flag.PrintDefaults()
		fmt.Println("need group name")
		os.Exit(4)
	}

	config := make(map[string]interface{})
	config["bootstrap.servers"] = *brokers
	config["topic"] = *topic
	config["group.id"] = *groupID
	if *clientID != "" {
		config["client.id"] = *clientID
	}
	config["session.timeout.ms"] = *sessionTimeout
	config["fetch.max.wait.ms"] = *maxWaitTime
	config["fetch.min.bytes"] = *minBytes
	config["max.partition.fetch.bytes"] = *maxBytes
	config["offsets.storage"] = *offsetsStorage
	config["connectTimeout"] = *connectTimeout
	config["timeout"] = *timeout

	c, err := healer.NewGroupConsumer(config)
	if err != nil {
		glog.Fatalf("could not init GroupConsumer:%s", err)
	}

	messages, err := c.Consume(*fromBeginning, nil)
	defer c.Close()
	if err != nil {
		glog.Fatalf("could not get messages channel:%s", err)
	}

	for i := 0; i < *maxMessages; i++ {
		message := <-messages
		fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
	}
}

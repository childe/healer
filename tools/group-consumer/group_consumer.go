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
	clientID       = flag.String("clientID", "healer", "The ID of this client.")
	groupID        = flag.String("groupID", "", "REQUIRED: The ID of this client.")
	minBytes       = flag.Int("min-bytes", 1, "The fetch size of each request.")
	fromBeginning  = flag.Bool("from-beginning", false, "default false")
	maxWaitTime    = flag.Int("max-wait-ms", 10000, "The max amount of time(ms) each fetch request waits(default 10000).")
	maxMessages    = flag.Int("max-messages", math.MaxInt32, "The number of messages to consume (default: 2147483647)")
	maxBytes       = flag.Int("max-bytes", math.MaxInt32, "The maximum bytes to include in the message set for this partition. This helps bound the size of the response.")
	connectTimeout = flag.Int("connect-timeout", 10, "default 10 Second. connect timeout to broker")
	timeout        = flag.Int("timeout", 30, "default 30 Second. read timeout from connection to broker")
	sessionTimeout = flag.Int("sessionTimeout", 30000, "The coordinator considers the consumer dead if it receives no heartbeat after this timeout in ms.")
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

	c, err := healer.NewGroupConsumer(*brokers, *topic, *clientID, *groupID, *sessionTimeout, int32(*maxWaitTime), int32(*minBytes), int32(*maxBytes))
	if err != nil {
		glog.Fatalf("could not init GroupConsumer:%s", err)
	}

	messages, err := c.Consume(*fromBeginning)
	if err != nil {
		glog.Fatalf("could not get messages channel:%s", err)
	}

	for i := 0; i < *maxMessages; i++ {
		message := <-messages
		fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
	}
}

package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"os/signal"
	"strings"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	topic       = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	groupID     = flag.String("group.id", "", "REQUIRED")
	maxMessages = flag.Int("max-messages", math.MaxInt32, "The number of messages to consume")

	brokers        = flag.String("brokers", "", "REQUIRED: The list of hostname and port of the server to connect to")
	config         = flag.String("config", "", "XX=YY,AA=ZZ")
	consumerConfig = map[string]interface{}{"bootstrap.servers": brokers}
)

func main() {
	flag.Parse()

	defer glog.Flush()

	if *topic == "" {
		fmt.Println("need topic name")
		os.Exit(4)
	}

	if *groupID == "" {
		fmt.Println("need group name")
		os.Exit(4)
	}

	consumerConfig["group.id"] = *groupID

	for _, kv := range strings.Split(*config, ",") {
		if kv = strings.Trim(kv, " "); kv == "" {
			continue
		}
		t := strings.SplitN(kv, "=", 2)
		if len(t) != 2 {
			glog.Errorf("invalid config : %s", kv)
			os.Exit(4)
		}
		consumerConfig[t[0]] = t[1]
	}
	cConfig, err := healer.GetConsumerConfig(consumerConfig)
	if err != nil {
		glog.Errorf("could not init consumer config: %s", err)
		os.Exit(4)
	}

	c, err := healer.NewGroupConsumer(*topic, cConfig)
	if err != nil {
		glog.Fatalf("could not create GroupConsumer: %s", err)
	}

	signalC := make(chan os.Signal, 1)
	signal.Notify(signalC, os.Interrupt)

	done := make(chan bool, 1)
	doneBySignal := make(chan bool, 1)

	go func() {
		<-signalC
		signal.Stop(signalC)
		glog.Info("closing group consumer...")
		c.Close()
		doneBySignal <- true
	}()

	messages, err := c.Consume(nil)

	if err != nil {
		glog.Fatalf("failed to consume: %s", err)
	}

	go func() {
		for i := 0; i < *maxMessages; i++ {
			message := <-messages
			fmt.Printf("%s:%d:%d:%s\n", message.TopicName, message.PartitionID, message.Message.Offset, message.Message.Value)
		}
		done <- true
	}()

	select {
	case <-done:
		c.Close()
		return
	case <-doneBySignal:
		return
	}
}

package main

//TODO maxBytes

import (
	"fmt"
	"math"
	"os"
	"os/signal"
	"strings"

	goflag "flag"

	flag "github.com/spf13/pflag"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	topic       = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	partition   = flag.Int("partition", 0, "The partition to consume from.")
	offset      = flag.Int64("offset", -2, "The offset id to consume from, default to -2 which means from beginning; while value -1 means from end(default -2).")
	stopOffset  = flag.Int64("stop-offset", 0, "fetch messages until stop-offset")
	maxMessages = flag.Int("max-messages", math.MaxInt32, "The number of messages to consume (default: 2147483647)")
	printOffset = flag.Bool("printoffset", true, "print offset before message")

	brokers = flag.String("brokers", "", "REQUIRED: The list of hostname and port of the server to connect to")

	config         = flag.String("config", "", "XX=YY,AA=ZZ")
	consumerConfig = map[string]interface{}{"bootstrap.servers": brokers}
)

func init() {
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)
}

func main() {
	flag.Parse()
	var returnCode = 0

	defer os.Exit(returnCode)
	defer glog.Flush()

	if *topic == "" {
		flag.PrintDefaults()
		fmt.Println("need topic name")
		returnCode = 4
		return
	}

	for _, kv := range strings.Split(*config, ",") {
		if strings.Trim(kv, " ") == "" {
			continue
		}
		t := strings.SplitN(kv, "=", 2)
		if len(t) != 2 {
			glog.Errorf("invalid config : %s", kv)
			returnCode = 4
			return
		}
		consumerConfig[t[0]] = t[1]
	}
	cConfig, err := healer.GetConsumerConfig(consumerConfig)
	if err != nil {
		glog.Errorf("config error : %s", err)
		returnCode = 4
		return
	}

	glog.V(10).Infof("config : %+v", cConfig)
	simpleConsumer, err := healer.NewSimpleConsumer(*topic, int32(*partition), cConfig)
	if err != nil {
		glog.Errorf("create simple consumer error: %s", err)
		returnCode = 5
		return
	}

	signalC := make(chan os.Signal, 1)
	signal.Notify(signalC, os.Interrupt)

	go func() {
		<-signalC
		simpleConsumer.Stop()
		glog.Flush()
		os.Exit(returnCode)
	}()

	messages, err := simpleConsumer.Consume(*offset, nil)
	if err != nil {
		glog.Errorf("consumer messages error: %s", err)
		returnCode = 5
		return
	}

	for i := 0; i < *maxMessages; i++ {
		message := <-messages
		if message.Error != nil {
			fmt.Printf("message error:%s\n", message.Error)
			break
		}
		if *stopOffset > 0 && message.Message.Offset >= *stopOffset {
			break
		}
		if *printOffset {
			fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
		} else {
			fmt.Printf("%s\n", message.Message.Value)
		}
	}
}

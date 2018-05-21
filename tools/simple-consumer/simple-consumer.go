package main

//TODO maxBytes

import (
	"fmt"
	"math"
	"os"

	goflag "flag"

	flag "github.com/spf13/pflag"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	consumerConfig = healer.DefaultConsumerConfig()

	topic       = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	partition   = flag.Int("partition", 0, "The partition to consume from.")
	offset      = flag.Int64("offset", -2, "The offset id to consume from, default to -2 which means from beginning; while value -1 means from end(default -2).")
	maxMessages = flag.Int("max-messages", math.MaxInt32, "The number of messages to consume (default: 2147483647)")

	printOffset = true
)

func init() {
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)

	flag.StringVar(&consumerConfig.BootstrapServers, "bootstrap.servers", "", "REQUIRED: The list of hostname and port of the server to connect to")
	flag.StringVar(&consumerConfig.ClientID, "client.id", consumerConfig.ClientID, "The ID of this client.")
	flag.Int32Var(&consumerConfig.FetchMinBytes, "fetch.min.bytes", consumerConfig.FetchMinBytes, "The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will wait for that much data to accumulate before answering the request.")
	flag.Int32Var(&consumerConfig.FetchMaxBytes, "fetch.max.bytes", consumerConfig.FetchMaxBytes, "The maximum bytes to include in the message set for this partition. This helps bound the size of the response")
	flag.Int32Var(&consumerConfig.FetchMaxWaitMS, "fetch.max.wait.ms", consumerConfig.FetchMaxWaitMS, "The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes")
	flag.IntVar(&consumerConfig.ConnectTimeoutMS, "connect.timeout.ms", consumerConfig.ConnectTimeoutMS, "connect timeout to broker")
	flag.IntVar(&consumerConfig.TimeoutMS, "timeout.ms", consumerConfig.TimeoutMS, "read timeout from connection to broker")
	flag.BoolVar(&printOffset, "printoffset", printOffset, "if print offset before message")
}

func main() {
	flag.Parse()

	if *topic == "" {
		flag.PrintDefaults()
		fmt.Println("need topic name")
		os.Exit(4)
	}

	simpleConsumer, err := healer.NewSimpleConsumer(*topic, int32(*partition), consumerConfig)
	if err != nil {
		glog.Errorf("crate simple consumer error: %s", err)
		os.Exit(5)
	}

	messages, err := simpleConsumer.Consume(*offset, nil)
	if err != nil {
		glog.Fatal(err)
	}

	for i := 0; i < *maxMessages; i++ {
		message := <-messages
		if message.Error != nil {
			fmt.Printf("messag error:%s\n", message.Error)
			return
		}
		if printOffset {
			fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
		} else {
			fmt.Printf("%s\n", message.Message.Value)
		}
	}
}

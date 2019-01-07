package main

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	goflag "flag"

	flag "github.com/spf13/pflag"

	"github.com/childe/healer"
	"github.com/golang/glog"
)

var (
	consumerConfig = healer.DefaultConsumerConfig()

	topic       = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	partitions  = flag.String("partitions", "", "comma separated. consume all partitions if not set")
	maxMessages = flag.Int("max-messages", math.MaxInt32, "The number of messages to consume (default: 2147483647)")
)

func init() {
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)

	flag.BoolVar(&consumerConfig.FromBeginning, "from-beginning", false, "default false")
	flag.StringVar(&consumerConfig.BootstrapServers, "bootstrap.servers", "", "REQUIRED: The list of hostname and port of the server to connect to")
	flag.StringVar(&consumerConfig.ClientID, "client.id", consumerConfig.ClientID, "The ID of this client.")
	flag.StringVar(&consumerConfig.GroupID, "group.id", consumerConfig.GroupID, "group ID. would commit offset if group.id is set")
	flag.Int32Var(&consumerConfig.FetchMinBytes, "fetch.min.bytes", consumerConfig.FetchMinBytes, "The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will wait for that much data to accumulate before answering the request.")
	flag.Int32Var(&consumerConfig.FetchMaxBytes, "fetch.max.bytes", consumerConfig.FetchMaxBytes, "The maximum bytes to include in the message set for this partition. This helps bound the size of the response")
	flag.Int32Var(&consumerConfig.FetchMaxWaitMS, "fetch.max.wait.ms", consumerConfig.FetchMaxWaitMS, "The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes")
	flag.IntVar(&consumerConfig.ConnectTimeoutMS, "connect.timeout.ms", consumerConfig.ConnectTimeoutMS, "connect timeout to broker")
	flag.IntVar(&consumerConfig.TimeoutMS, "timeout.ms", consumerConfig.TimeoutMS, "read timeout from connection to broker")
}

func main() {
	flag.Parse()

	if *topic == "" {
		flag.PrintDefaults()
		fmt.Println("need topic name")
		os.Exit(4)
	}

	var (
		consumer *healer.Consumer
		err      error
	)

	if *partitions == "" {
		consumer, err = healer.NewConsumer(consumerConfig, *topic)
	} else {
		assign := make(map[string][]int)
		assign[*topic] = make([]int, 0)
		for _, p := range strings.Split(*partitions, ",") {
			pid, err := strconv.Atoi(p)
			if err != nil {
				glog.Errorf("invalid partitions: %s", err)
				os.Exit(4)
			}
			assign[*topic] = append(assign[*topic], pid)
		}
		consumer, err = healer.NewConsumer(consumerConfig)
		consumer.Assign(assign)
	}

	if err != nil {
		glog.Errorf("create consumer error: %s", err)
		os.Exit(5)
	}

	messages, err := consumer.Consume()
	if err != nil {
		glog.Fatal(err)
	}

	for i := 0; i < *maxMessages; i++ {
		message := <-messages
		fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
	}
}

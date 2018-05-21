package main

import (
	"fmt"
	"math"
	"os"

	goflag "flag"

	flag "github.com/spf13/pflag"

	"github.com/golang/glog"
	"github.com/childe/healer"
)

var (
	consumerConfig = healer.DefaultConsumerConfig()

	topic         = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	maxMessages   = flag.Int("max-messages", math.MaxInt32, "The number of messages to consume (default: 2147483647)")
	fromBeginning = flag.Bool("from-beginning", false, "default false")
)

func init() {
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)

	flag.StringVar(&consumerConfig.BootstrapServers, "bootstrap.servers", "", "REQUIRED: The list of hostname and port of the server to connect to")
	flag.StringVar(&consumerConfig.ClientID, "client.id", consumerConfig.ClientID, "The ID of this client.")
	flag.StringVar(&consumerConfig.GroupID, "group.id", "", "REQUIRED")
	flag.Int32Var(&consumerConfig.FetchMinBytes, "fetch.min.bytes", consumerConfig.FetchMinBytes, "The minimum amount of data the server should return for a fetch request. If insufficient data is available the request will wait for that much data to accumulate before answering the request.")
	flag.Int32Var(&consumerConfig.FetchMaxBytes, "fetch.max.bytes", consumerConfig.FetchMaxBytes, "The maximum bytes to include in the message set for this partition. This helps bound the size of the response")
	flag.Int32Var(&consumerConfig.FetchMaxWaitMS, "fetch.max.wait.ms", consumerConfig.FetchMaxWaitMS, "The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy fetch.min.bytes")
	flag.Int32Var(&consumerConfig.SessionTimeoutMS, "session.timeout.ms", consumerConfig.SessionTimeoutMS, "The timeout used to detect failures when using Kafka's group management facilities.")
	flag.IntVar(&consumerConfig.OffsetsStorage, "offsets.storage", 1, "Select where offsets should be stored (0 zookeeper or 1 kafka)")
	flag.BoolVar(&consumerConfig.AutoCommit, "auto.commit.enable", consumerConfig.AutoCommit, "If true, periodically commit the offset of messages already fetched by the consumer. This committed offset will be used when the process fails as the position from which the new consumer will begin")
	flag.IntVar(&consumerConfig.AutoCommitIntervalMS, "auto.commit.interval.ms", consumerConfig.AutoCommitIntervalMS, "The frequency in ms that the consumer offsets are committed")
	flag.BoolVar(&consumerConfig.CommitAfterFetch, "commit.after.fetch", consumerConfig.CommitAfterFetch, "commit offset after every fetch request")
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

	consumer, err := healer.NewConsumer(*topic, consumerConfig)
	if err != nil {
		glog.Errorf("create consumer error: %s", err)
		os.Exit(5)
	}

	messages, err := consumer.Consume(*fromBeginning)
	if err != nil {
		glog.Fatal(err)
	}

	for i := 0; i < *maxMessages; i++ {
		message := <-messages
		fmt.Printf("%d: %s\n", message.Message.Offset, message.Message.Value)
	}
}

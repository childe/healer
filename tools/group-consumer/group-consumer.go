package main

import (
	"fmt"
	"math"
	"os"
	"os/signal"
	"time"

	goflag "flag"

	"github.com/childe/healer"
	"github.com/golang/glog"
	flag "github.com/spf13/pflag"
)

var (
	consumerConfig = healer.DefaultConsumerConfig()
	topic          = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	maxMessages    = flag.Int("max-messages", math.MaxInt32, "The number of messages to consume")
)

func init() {
	flag.CommandLine.AddGoFlagSet(goflag.CommandLine)

	flag.BoolVar(&consumerConfig.FromBeginning, "from.beginning", false, "")
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
	flag.IntVar(&consumerConfig.MetadataMaxAgeMS, "metadata.max.age.ms", consumerConfig.MetadataMaxAgeMS, "The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions.")
	flag.BoolVar(&consumerConfig.CommitAfterFetch, "commit.after.fetch", consumerConfig.CommitAfterFetch, "commit offset after every fetch request")
	flag.IntVar(&consumerConfig.ConnectTimeoutMS, "connect.timeout.ms", consumerConfig.ConnectTimeoutMS, "connect timeout to broker")
	flag.IntVar(&consumerConfig.TimeoutMS, "timeout.ms", consumerConfig.TimeoutMS, "read timeout from connection to broker")
}

func main() {
	flag.Parse()

	if *topic == "" {
		fmt.Println("need topic name")
		os.Exit(4)
	}

	if consumerConfig.GroupID == "" {
		fmt.Println("need group name")
		os.Exit(4)
	}

	c, err := healer.NewGroupConsumer(*topic, consumerConfig)
	if err != nil {
		glog.Fatalf("could not init GroupConsumer: %s", err)
	}

	signalC := make(chan os.Signal, 1)
	signal.Notify(signalC, os.Interrupt)
	go func() {
		<-signalC
		signal.Stop(signalC)
		c.AwaitClose(time.Second * 10)
		os.Exit(0)
	}()

	messages, err := c.Consume(nil)
	defer c.Close()
	if err != nil {
		glog.Fatalf("could not get messages channel:%s", err)
	}

	for i := 0; i < *maxMessages; i++ {
		message := <-messages
		fmt.Printf("%s:%d:%d:%s\n", message.TopicName, message.PartitionID, message.Message.Offset, message.Message.Value)
	}
}

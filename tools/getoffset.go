package main

import (
	"flag"
	"fmt"
	"github.com/gokafka"
	"log"
	"net"
	"os"
	"time"
)

var (
	brokerList         = flag.String("brokers", "127.0.0.1:9092", "The comma separated list of brokers in the Kafka cluster.")
	topic              = flag.String("topic", "", "REQUIRED")
	timeValue          = flag.Int("time", -3, "REQUIRED. time")
	maxNumberOfOffsets = flag.Int("maxoffsets", 0, "REQUIRED. time")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()
	//flag.PrintDefaults()

	correlationId := uint32(os.Getpid())

	requestHeader := &gokafka.RequestHeader{
		ApiKey:        gokafka.API_OffsetRequest,
		ApiVersion:    0,
		CorrelationId: correlationId,
		ClientId:      "gokafka_getoffset",
	}

	partitionOffsetRequestInfos := make(map[uint32]*gokafka.PartitionOffsetRequestInfo)
	partitionOffsetRequestInfos[0] = &gokafka.PartitionOffsetRequestInfo{
		Time:               -1,
		MaxNumberOfOffsets: 1000,
	}
	topicOffsetRequestInfos := make(map[string]map[uint32]*gokafka.PartitionOffsetRequestInfo)
	topicOffsetRequestInfos[*topic] = partitionOffsetRequestInfos

	offsetReqeust := &gokafka.OffsetReqeust{
		RequestHeader: requestHeader,
		ReplicaId:     -1,
		RequestInfo:   topicOffsetRequestInfos,
	}

	payload := offsetReqeust.Encode()

	dialer := net.Dialer{
		Timeout:   time.Second * 5,
		KeepAlive: time.Hour * 2,
	}
	addr := net.JoinHostPort(*brokerList, "9092")
	conn, connErr := dialer.Dial("tcp", addr)
	if connErr != nil {
		fmt.Println("dial error")
		fmt.Println(connErr)
		return
	}
	conn.Write(payload)

	responsePayload := make([]byte, 1024)
	length, _ := conn.Read(responsePayload)

	offsetResponse := &gokafka.OffsetResponse{}
	offsetResponse.Decode(responsePayload[:length])
	for topic, partions := range offsetResponse.Info {
		fmt.Println(topic)
		for _, b := range partions {
			fmt.Println(*b)
		}
	}
}

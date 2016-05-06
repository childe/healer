package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/childe/gokafka"
)

var (
	brokerList = flag.String("brokers", "127.0.0.1:9092", "<hostname:port,...,hostname:port> The comma separated list of brokers in the Kafka cluster. (default: 127.0.0.1:9092)")
	topic      = flag.String("topic", "", "REQUIRED: The topic to get offset from.")
	timeValue  = flag.Int64("time", -1, "timestamp/-1(latest)/-2(earliest). timestamp of the offsets before that.(default: -1) ")
	offsets    = flag.Uint("offsets", 1, "number of offsets returned (default: 1)")
	clientID   = flag.String("clientID", "gokafka", "The ID of this client.")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *topic == "" {
		fmt.Println("need topic!")
		flag.PrintDefaults()
	}

	correlationID := int32(os.Getpid())

	requestHeader := &gokafka.RequestHeader{
		ApiKey:        gokafka.API_OffsetRequest,
		ApiVersion:    0,
		CorrelationId: correlationID,
		ClientId:      *clientID,
	}

	partitionOffsetRequestInfos := make(map[uint32]*gokafka.PartitionOffsetRequestInfo)
	partitionOffsetRequestInfos[0] = &gokafka.PartitionOffsetRequestInfo{
		Time:               *timeValue,
		MaxNumberOfOffsets: uint32(*offsets),
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

	conn, connErr := dialer.Dial("tcp", *brokerList)
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

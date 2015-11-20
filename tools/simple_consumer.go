package main

import (
	"encoding/binary"
	"encoding/json"
	"flag"
	"github.com/gokafka"
	"io"
	"log"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	brokerList = flag.String("brokers", "127.0.0.1:9092", "The list of hostname and port of the server to connect to.")
	topic      = flag.String("topic", "", "REQUIRED: The topic to consume from.")
	partition  = flag.Int("partition", 0, "The partition to consume from.")
	offset     = flag.Int64("offset", 0, "The offset id to consume from, default to -2 which means from beginning; while value -1 means from end.")
	clientId   = flag.String("clientId", "gokafkaShell", "The ID of this client.")
	minBytes   = flag.Int("min-bytes", 1, "The fetch size of each request.")
	maxWaitMs  = flag.Int("max-wait-ms", 1000, "The max amount of time each fetch request waits.")
	maxBytes   = flag.Int("max-bytes", math.MaxInt32, "The maximum bytes to include in the message set for this partition. This helps bound the size of the response.")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *topic == "" {
		flag.PrintDefaults()
	}

	pid := os.Getpid()

	var metadataResponse *gokafka.MetadataResponse = nil
	for _, broker := range strings.Split(*brokerList, ",") {
		_metadataResponse, err := gokafka.GetMetaData(broker, *topic, int32(pid), *clientId)
		if err != nil {
			logger.Println(err)
			continue
		}
		metadataResponse = _metadataResponse
		break
	}

	if metadataResponse == nil {
		logger.Fatalf("could not get metadata of topic[%s] from %s", *topic, *brokerList)
	}

	partitionMetadatas := metadataResponse.TopicMetadatas[0].PartitionMetadatas
	//find leader
	var leader int32
	for _, partitionMetadata := range partitionMetadatas {
		if partitionMetadata.PartitionId == int32(*partition) {
			leader = partitionMetadata.Leader
			break
		}
	}

	var (
		host string
		port int32
	)
	for _, broker := range metadataResponse.Brokers {
		if broker.NodeId == leader {
			host = broker.Host
			port = broker.Port
		}
	}
	logger.Printf("leader of %s:%d is %s:%d", *topic, *partition, host, port)

	correlationId := int32(0)
	partitonBlock := &gokafka.PartitonBlock{
		Partition:   int32(*partition),
		FetchOffset: *offset,
		MaxBytes:    int32(*maxBytes),
	}
	fetchRequest := gokafka.FetchRequest{
		ReplicaId:   -1,
		MaxWaitTime: int32(*maxWaitMs),
		MinBytes:    int32(*minBytes),
		Topics:      map[string][]*gokafka.PartitonBlock{*topic: []*gokafka.PartitonBlock{partitonBlock}},
	}
	fetchRequest.RequestHeader = &gokafka.RequestHeader{
		ApiKey:        gokafka.API_FetchRequest,
		ApiVersion:    0,
		CorrelationId: correlationId,
		ClientId:      *clientId,
	}
	s, _ := json.MarshalIndent(fetchRequest, "", "  ")
	logger.Println(string(s))

	leaderAddr := net.JoinHostPort(host, strconv.Itoa(int(port)))
	conn, err := net.DialTimeout("tcp", leaderAddr, time.Second*5)
	if err != nil {
		logger.Fatalln(err)
	}

	payload := fetchRequest.Encode()
	conn.Write(payload)
	buf := make([]byte, 4)
	_, err = conn.Read(buf)
	logger.Println(buf)

	if err != nil {
		logger.Fatalln(err)
	}
	responseLength := int(binary.BigEndian.Uint32(buf))
	logger.Println(responseLength)
	buf = make([]byte, responseLength)

	offset := 0
	for {
		length, err := conn.Read(buf[offset:])
		logger.Println(length)
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Fatalln(err)
		}
		offset += length
		if offset > responseLength {
			logger.Fatalln("read more than needed")
		}
	}
	logger.Println(buf)
	returnCorrelationId := int32(binary.BigEndian.Uint32(buf))
	if returnCorrelationId != correlationId {
		logger.Fatalln("CorrelationId NOT match")
	}
	fetchResponse, err := gokafka.DecodeFetchResponse(buf[4:])
	if err != nil {
		logger.Fatalln(err)
	}
	conn.Close()

	s, _ = json.MarshalIndent(fetchResponse, "", "  ")
	logger.Println(string(s))

	//for _, topic := range fetchResponse.Topics {
	//for _, topicBlock := range topic.TopicBlocks {
	//for _, message := range topicBlock.MessageSet {
	//logger.Println(string(message.Value))
	//}
	//}
	//}
}

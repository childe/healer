package gokafka

import (
	"encoding/binary"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var logger = log.New(os.Stderr, "", log.LstdFlags)

type SimpleConsumer struct {
	ClientId    string
	Brokers     string
	TopicName   string
	Partition   int32
	FetchOffset int64
	MaxBytes    int32
	MaxWaitTime int32
	MinBytes    int32
}

func (simpleConsumer *SimpleConsumer) consume(messages chan Message) {
	var (
		metadataResponse *MetadataResponse = nil
		err              error
	)
	pid := os.Getpid()
	for _, broker := range strings.Split(simpleConsumer.Brokers, ",") {
		metadataResponse, err = GetMetaData(broker, simpleConsumer.TopicName, int32(pid), simpleConsumer.ClientId)
		if err != nil {
			logger.Println(err)
		} else {
			break
		}
	}

	if metadataResponse == nil {
		logger.Fatalf("could not get metadata of topic[%s] from %s", simpleConsumer.TopicName, simpleConsumer.TopicName)
	}

	partitionMetadatas := metadataResponse.TopicMetadatas[0].PartitionMetadatas
	//find leader
	var leader int32
	for _, partitionMetadata := range partitionMetadatas {
		if partitionMetadata.PartitionId == simpleConsumer.Partition {
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
	logger.Printf("leader of %s:%d is %s:%d", simpleConsumer.TopicName, simpleConsumer.Partition, host, port)
	leaderAddr := net.JoinHostPort(host, strconv.Itoa(int(port)))
	conn, err := net.DialTimeout("tcp", leaderAddr, time.Second*5)
	if err != nil {
		logger.Fatalln(err)
	}
	defer func() { conn.Close() }()

	correlationId := int32(0)
	partitonBlock := &PartitonBlock{
		Partition:   simpleConsumer.Partition,
		FetchOffset: simpleConsumer.FetchOffset,
		MaxBytes:    simpleConsumer.MaxBytes,
	}
	fetchRequest := FetchRequest{
		ReplicaId:   -1,
		MaxWaitTime: simpleConsumer.MaxWaitTime,
		MinBytes:    simpleConsumer.MinBytes,
		Topics:      map[string][]*PartitonBlock{simpleConsumer.TopicName: []*PartitonBlock{partitonBlock}},
	}
	fetchRequest.RequestHeader = &RequestHeader{
		ApiKey:        API_FetchRequest,
		ApiVersion:    0,
		CorrelationId: correlationId,
		ClientId:      simpleConsumer.ClientId,
	}
	for {
		payload := fetchRequest.Encode()
		conn.Write(payload)
		buf := make([]byte, 4)
		_, err = conn.Read(buf)
		//logger.Println(buf)

		if err != nil {
			logger.Fatalln(err)
		}
		responseLength := int(binary.BigEndian.Uint32(buf))
		logger.Println(responseLength)
		buf = make([]byte, responseLength)

		readLength := 0
		for {
			length, err := conn.Read(buf[readLength:])
			logger.Println(length)
			if err == io.EOF {
				break
			}
			if err != nil {
				logger.Fatalln(err)
			}
			readLength += length
			if readLength > responseLength {
				logger.Fatalln("fetch more data than needed")
			}
		}
		fetchResponse, err := DecodeFetchResponse(buf[4:])
		if err != nil {
			logger.Fatalln(err)
		}

		var offset int64
		for _, fetchResponsePiece := range fetchResponse {
			for _, topicData := range fetchResponsePiece.TopicDatas {
				if topicData.ErrorCode != 0 {
					logger.Printf("failed to fetch data from topic[%s] partition[%d]\n", simpleConsumer.TopicName, simpleConsumer.Partition)
				}
				for _, message := range topicData.MessageSet {
					offset = message.Offset
					messages <- message
				}
			}
		}
		log.Printf("offset is %d\n", offset)
	}
}

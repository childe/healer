package healer

import (
	"encoding/binary"
	"io"
	"net"
	"time"
)

type SimpleProducer struct {
	Config struct {
		ClientId     string
		Broker       string
		TopicName    string
		Partition    int32
		RequiredAcks int16
		Timeout      int32
		MessageCap   int32
	}
	CorrelationID  uint32
	MessageSetSize int32
	MessageSet     MessageSet
}

func (simpleProducer *SimpleProducer) AddMessage(message *Message) {
	simpleProducer.MessageSet[simpleProducer.MessageSetSize] = message
	simpleProducer.MessageSetSize++
	if simpleProducer.MessageSetSize == simpleProducer.Config.MessageCap {
		simpleProducer.Emit()
	}
}

func (simpleProducer *SimpleProducer) Emit() {
	simpleProducer.emit()
	simpleProducer.CorrelationID++
	simpleProducer.MessageSetSize = 0
}

func (simpleProducer *SimpleProducer) emit() {
	conn, err := net.DialTimeout("tcp", simpleProducer.Config.Broker, time.Second*5)
	if err != nil {
		//logger.Fatalln(err)
	}
	defer func() { conn.Close() }()

	produceRequest := ProduceRequest{
		RequiredAcks: simpleProducer.Config.RequiredAcks,
		Timeout:      simpleProducer.Config.Timeout,
	}
	produceRequest.RequestHeader = &RequestHeader{
		ApiKey:        API_ProduceRequest,
		ApiVersion:    0,
		CorrelationID: simpleProducer.CorrelationID,
		ClientId:      simpleProducer.Config.ClientId,
	}

	produceRequest.TopicBlocks = make([]struct {
		TopicName      string
		PartitonBlocks []struct {
			Partition      int32
			MessageSetSize int32
			MessageSet     MessageSet
		}
	}, 1)
	produceRequest.TopicBlocks[0].TopicName = simpleProducer.Config.TopicName
	produceRequest.TopicBlocks[0].PartitonBlocks = make([]struct {
		Partition      int32
		MessageSetSize int32
		MessageSet     MessageSet
	}, 1)

	produceRequest.TopicBlocks[0].PartitonBlocks[0].Partition = 0
	produceRequest.TopicBlocks[0].PartitonBlocks[0].MessageSetSize = simpleProducer.MessageSetSize
	produceRequest.TopicBlocks[0].PartitonBlocks[0].MessageSet = simpleProducer.MessageSet[:simpleProducer.MessageSetSize]

	for {
		payload := produceRequest.Encode()
		//logger.Println("request length", len(payload))
		//logger.Println(payload)
		conn.Write(payload)
		buf := make([]byte, 4)
		_, err = conn.Read(buf)
		//logger.Println(buf)

		if err != nil {
			//logger.Fatalln(err)
		}
		responseLength := int(binary.BigEndian.Uint32(buf))
		//logger.Println("responseLength:", responseLength)
		buf = make([]byte, responseLength)

		readLength := 0
		for {
			length, err := conn.Read(buf[readLength:])
			//logger.Println("length", length)
			if err == io.EOF {
				break
			}
			if err != nil {
				//logger.Fatalln(err)
			}
			readLength += length
			if readLength > responseLength {
				//logger.Fatalln("fetch more data than needed")
			}
		}
		//logger.Println(buf)
		//correlationID := int32(binary.BigEndian.Uint32(buf))
		//logger.Println("correlationID", correlationID)
	}
}

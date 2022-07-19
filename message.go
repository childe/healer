package healer

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"

	lz4 "github.com/bkaradzic/go-lz4"
	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/golang/glog"
)

var errUncompleteRecord = errors.New("Uncomplete Record, The last bytes are not enough to decode the record")

// RecordHeader is concluded in Record
type RecordHeader struct {
	headerKeyLength   int32
	headerKey         string
	headerValueLength int32
	Value             []byte
}

func decodeHeader(payload []byte) (header RecordHeader, offset int) {
	keyLength, o := binary.Varint(payload[offset:])
	header.headerKeyLength = int32(keyLength)
	offset += o
	header.headerKey = string(payload[offset : offset+int(header.headerKeyLength)])
	offset += int(header.headerKeyLength)

	valueLength, o := binary.Varint(payload[offset:])
	header.headerValueLength = int32(valueLength)
	offset += o
	header.Value = make([]byte, valueLength)
	offset += copy(header.Value, payload[offset:offset+int(header.headerValueLength)])
	return
}

// Record is element of Records
type Record struct {
	length         int32
	attributes     int8
	timestampDelta int64
	offsetDelta    int32
	keyLength      int32
	key            []byte
	valueLen       int32
	value          []byte
	Headers        []RecordHeader
}

// DecodeToRecord decodes the struct Record from the given payload.
func DecodeToRecord(payload []byte) (record Record, offset int, err error) {
	length, o := binary.Varint(payload)
	glog.V(15).Infof("length: %d", length)
	glog.V(15).Infof("playload length: %d", len(payload))
	if length == 0 || len(payload[o:]) < int(length) {
		return record, o, errUncompleteRecord
	}
	record.length = int32(length)
	offset += o

	record.attributes = int8(payload[offset])
	offset++

	timestampDelta, o := binary.Varint(payload[offset:])
	glog.V(15).Infof("timestampDelta: %d", timestampDelta)
	record.timestampDelta = int64(timestampDelta)
	offset += o

	offsetDelta, o := binary.Varint(payload[offset:])
	glog.V(15).Infof("offsetDelta: %d", offsetDelta)
	record.offsetDelta = int32(offsetDelta)
	offset += o

	keyLength, o := binary.Varint(payload[offset:])
	glog.V(15).Infof("keyLength: %d", keyLength)
	record.keyLength = int32(keyLength)
	offset += o

	if keyLength > 0 {
		record.key = make([]byte, keyLength)
		offset += copy(record.key, payload[offset:offset+int(record.keyLength)])
	}

	valueLen, o := binary.Varint(payload[offset:])
	glog.V(15).Infof("valueLen: %d", valueLen)
	record.valueLen = int32(valueLen)
	offset += o
	if valueLen > 0 {
		record.value = make([]byte, valueLen)
		offset += copy(record.value, payload[offset:offset+int(record.valueLen)])
	}

	headerCount, o := binary.Varint(payload[offset:])
	glog.V(15).Infof("headerCount: %d", headerCount)
	offset += o
	if headerCount > 0 {
		record.Headers = make([]RecordHeader, headerCount)
		for i := int64(0); i < headerCount; i++ {
			record.Headers[i], o = decodeHeader(payload[offset:])
			offset += o
		}
	}

	return
}

// Records is batch of Record
type Records struct {
	baseOffset           int64
	batchLength          int32
	partitionLeaderEpoch int32
	magic                int8
	crc                  int32
	attributes           int16
	lastOffsetDelta      int32
	baseTimestamp        int64
	maxTimestamp         int64
	producerID           int64
	producerEpoch        int16
	baseSequence         int32
	records              []Record
}

// FullMessage contains message value and topic and partition
type FullMessage struct {
	TopicName   string
	PartitionID int32
	Error       error
	Message     *Message
}

// Message is a message in a topic
type Message struct {
	Offset      int64
	MessageSize int32

	//Message
	Crc        uint32
	MagicByte  int8
	Attributes int8
	Timestamp  uint64
	Key        []byte
	Value      []byte
}

// MessageSet is a batch of messages
type MessageSet []*Message

const (
	COMPRESSION_NONE   int8 = 0
	COMPRESSION_GZIP   int8 = 1
	COMPRESSION_SNAPPY int8 = 2
	COMPRESSION_LZ4    int8 = 3
)

func (message *Message) decompress() ([]byte, error) {
	compression := message.Attributes & 7
	var rst []byte
	switch compression {
	case COMPRESSION_GZIP:
		reader, err := gzip.NewReader(bytes.NewReader(message.Value))
		if err != nil {
			return nil, err
		}
		if rst, err = ioutil.ReadAll(reader); err != nil && err != io.EOF {
			return nil, err
		} else {
			return rst, nil
		}
	case COMPRESSION_SNAPPY:
		return snappy.Decode(message.Value)
	case COMPRESSION_LZ4:
		return lz4.Decode(nil, message.Value)
	}
	return nil, fmt.Errorf("Unknown Compression Code %d", compression)
}

func (messageSet *MessageSet) Length() int {
	length := 0
	for _, message := range *messageSet {
		length += 26 + len(message.Key) + len(message.Value)
	}
	return length
}

func (messageSet *MessageSet) Encode(payload []byte, offset int) int {
	var i int32 = -1
	for _, message := range *messageSet {
		binary.BigEndian.PutUint64(payload[offset:], uint64(message.Offset))
		offset += 8

		binary.BigEndian.PutUint32(payload[offset:], uint32(14+len(message.Key)+len(message.Value)))
		offset += 4

		crcPosition := offset
		offset += 4

		payload[offset] = byte(message.MagicByte)
		offset++

		payload[offset] = byte(message.Attributes)
		offset++

		if message.Key == nil {
			binary.BigEndian.PutUint32(payload[offset:], uint32(i))
			offset += 4
		} else {
			binary.BigEndian.PutUint32(payload[offset:], uint32(len(message.Key)))
			offset += 4
			copy(payload[offset:], message.Key)
			offset += len(message.Key)
		}

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(message.Value)))
		offset += 4
		copy(payload[offset:], message.Value)
		offset += len(message.Value)

		message.Crc = crc32.ChecksumIEEE(payload[crcPosition+4 : offset])
		binary.BigEndian.PutUint32(payload[crcPosition:], message.Crc)
	}

	return offset
}

// DecodeToMessageSet decodes a MessageSet from a byte array.
// MessageSet is [offset message_size message], but it only decode one message in healer generally, loops inside decodeMessageSetMagic0or1.
// if message.Value is compressed, it will uncompress the value and returns an array of messages.
func DecodeToMessageSet(payload []byte) (MessageSet, error) {
	glog.V(15).Infof("payload %v", payload)
	messageSet := MessageSet{}
	var offset int = 0
	var err error

	for {
		if offset == len(payload) {
			break
		}

		message := &Message{}

		message.Offset = int64(binary.BigEndian.Uint64(payload[offset:]))
		glog.Infof("message.Offset %d", message.Offset)
		offset += 8

		message.MessageSize = int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4

		message.Crc = binary.BigEndian.Uint32(payload[offset:])
		offset += 4

		message.MagicByte = int8(payload[offset])
		glog.Infof("message.MagicByte %d", message.MagicByte)
		offset++

		message.Attributes = int8(payload[offset])
		offset++

		if message.MagicByte == 1 {
			message.Timestamp = binary.BigEndian.Uint64(payload[offset:])
			offset += 8
		}

		keyLength := int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		if keyLength == -1 {
			message.Key = nil
		} else {
			message.Key = make([]byte, keyLength)
			copy(message.Key, payload[offset:offset+int(keyLength)])
			offset += int(keyLength)
		}

		valueLength := int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		if valueLength == -1 {
			message.Value = nil
		} else {
			message.Value = make([]byte, valueLength)
			copy(message.Value, payload[offset:offset+int(valueLength)])
			offset += int(valueLength)
		}
		compression := message.Attributes & 0x07
		if compression != COMPRESSION_NONE {
			message.Value, err = message.decompress()
			if err != nil {
				// TODO go on to next message in the messageSet or stop?
				glog.Errorf("decompress message error:%s", err)
				//return messageSet, err
			}
		}

		// if crc check true, then go on decode to next level
		if err == nil && compression != COMPRESSION_NONE {
			if _messageSet, err := DecodeToMessageSet(message.Value); err != nil {
				// TODO go on to next message in the messageSet?
				glog.Errorf("decode message from value error:%s", err)
				return messageSet, err
			} else {
				messageSet = append(messageSet, _messageSet...)
			}
		} else {
			messageSet = append(messageSet, message)
		}
	}

	return messageSet, nil
}

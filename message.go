package healer

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/golang/glog"
	"github.com/golang/snappy"
	"github.com/klauspost/crc32"
)

/*
Message sets
One structure common to both the produce and fetch requests is the message set format. A message in kafka is a key-value pair with a small amount of associated metadata. A message set is just a sequence of messages with offset and size information. This format happens to be used both for the on-disk storage on the broker and the on-the-wire format.
A message set is also the unit of compression in Kafka, and we allow messages to recursively contain compressed message sets to allow batch compression.
N.B., MessageSets are not preceded by an int32 like other array elements in the protocol.

MessageSet => [Offset MessageSize Message]
  Offset => int64
  MessageSize => int32

Message format
Message => Crc MagicByte Attributes Key Value
  Crc => int32
  MagicByte => int8
  Attributes => int8
  Key => bytes
  Value => bytes

Offset			This is the offset used in kafka as the log sequence number. When the producer is sending messages it doesn't actually know the offset and can fill in any value here it likes.
Crc				The CRC is the CRC32 of the remainder of the message bytes. This is used to check the integrity of the message on the broker and consumer.
MagicByte		This is a version id used to allow backwards compatible evolution of the message binary format. The current value is 0.
Attributes		This byte holds metadata attributes about the message. The lowest 2 bits contain the compression codec used for the message. The other bits should be set to 0.
Key				The key is an optional message key that was used for partition assignment. The key can be null.
Value			The value is the actual message contents as an opaque byte array. Kafka supports recursive messages in which case this may itself contain a message set. The message can be null.
*/

//type Message struct {
//Crc        int32
//MagicByte  int8
//Attributes int8
//Key        []byte
//Value      []byte
//}

type FullMessage struct {
	TopicName   string
	PartitionID int32
	Message     *Message
}

type Message struct {
	Offset      int64
	MessageSize int32

	//Message
	Crc        uint32
	MagicByte  int8
	Attributes int8
	Key        []byte
	Value      []byte
}
type MessageSet []*Message

const (
	COMPRESSION_NONE   int8 = 0
	COMPRESSION_GZIP   int8 = 1
	COMPRESSION_SNAPPY int8 = 2
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
			glog.Info(err)
			glog.Info(rst)
			return nil, err
		} else {
			return rst, nil
		}
	case COMPRESSION_SNAPPY:
		return snappy.Decode(nil, message.Value)
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
	for _, message := range *messageSet {
		binary.BigEndian.PutUint64(payload[offset:], uint64(message.Offset))
		offset += 8

		binary.BigEndian.PutUint32(payload[offset:], uint32(14+len(message.Key)+len(message.Value)))
		offset += 4

		offset += 4
		crcPosition := offset

		payload[offset] = byte(message.MagicByte)
		offset += 1

		payload[offset] = byte(message.Attributes)
		offset += 1

		if message.Key == nil {
			var i int32 = -1
			binary.BigEndian.PutUint32(payload[offset:], uint32(i))
			offset += 4
		} else {
			binary.BigEndian.PutUint16(payload[offset:], uint16(len(message.Key)))
			offset += 2
			copy(payload[offset:], message.Key)
			offset += len(message.Key)
		}

		binary.BigEndian.PutUint32(payload[offset:], uint32(len(message.Value)))
		offset += 4
		copy(payload[offset:], message.Value)
		offset += len(message.Value)

		message.Crc = crc32.ChecksumIEEE(payload[crcPosition:offset])
		binary.BigEndian.PutUint32(payload[crcPosition-4:], message.Crc)
	}

	return offset
}

func DecodeToMessageSet(payload []byte) (MessageSet, int, error) {
	glog.V(10).Infof("DecodeToMessageSet %d", len(payload))
	messageSet := MessageSet{}
	var offset int = 0
	payloadLength := len(payload)
	for {
		glog.V(10).Infof("offset %d payloadLength %d", offset, payloadLength)

		if offset == payloadLength {
			return messageSet, offset, nil
		}

		//if payloadLength < offset+26 {
		//glog.V(5).Infof("response is truncated because of max-bytes parameter in fetch request(resopnseLength[%d] < offset[%d]+26).", payloadLength, offset)
		//if len(messageSet) == 0 {
		//return messageSet, offset, &maxBytesTooSmall
		//}
		//return messageSet, offset, &fetchResponseTruncated
		//}

		message := &Message{}
		message.Offset = int64(binary.BigEndian.Uint64(payload[offset:]))
		glog.V(10).Infof("message offset: %d", message.Offset)
		offset += 8

		message.MessageSize = int32(binary.BigEndian.Uint32(payload[offset:]))
		glog.V(10).Infof("message size: %d", message.MessageSize)
		offset += 4

		// TODO do NOT need
		//if payloadLength < offset+int(message.MessageSize) {
		//glog.V(5).Infof("response is truncated because of max-bytes parameter in fetch request(payloadLength[%d] < offset[%d]+messageSize[%d]).", payloadLength, offset, message.MessageSize)
		//if len(messageSet) == 0 {
		//return messageSet, offset, &maxBytesTooSmall
		//}
		//return messageSet, offset, &fetchResponseTruncated
		//}

		message.Crc = binary.BigEndian.Uint32(payload[offset:])
		offset += 4

		message.MagicByte = int8(payload[offset])
		offset++

		message.Attributes = int8(payload[offset])
		offset++

		keyLength := int32(binary.BigEndian.Uint32(payload[offset:]))
		glog.V(10).Infof("key length: %d", keyLength)
		offset += 4
		if keyLength == -1 {
			message.Key = nil
		} else {
			message.Key = make([]byte, keyLength)
			copy(message.Key, payload[offset:offset+int(keyLength)])
			glog.V(20).Infof("message key: %v", message.Key)
			offset += int(keyLength)
		}

		valueLength := int(binary.BigEndian.Uint32(payload[offset:]))
		glog.V(10).Infof("value length: %d", valueLength)
		offset += 4
		if valueLength == -1 {
			message.Value = nil
		} else {
			message.Value = make([]byte, valueLength)
			copy(message.Value, payload[offset:offset+valueLength])
			glog.V(20).Infof("message value: %v", message.Value)
			glog.V(15).Infof(string(message.Value))
			offset += valueLength
		}
		compression := message.Attributes & 7
		glog.V(10).Infof("compression %d", compression)
		if compression != COMPRESSION_NONE {
			value, err := message.decompress()
			if err != nil {
				glog.Fatal(err)
			}
			if _messageSet, _, err := DecodeToMessageSet(value); err != nil {
				return messageSet, offset, err
			} else {
				messageSet = append(messageSet, _messageSet...)
			}
		} else {
			messageSet = append(messageSet, message)
		}
	}
	return messageSet, offset, nil
}

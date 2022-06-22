package healer

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/golang/glog"
	"github.com/pierrec/lz4"
)

type abortedTransaction struct {
	producerID  int64
	firstOffset int64
}

// PartitionResponse stores partitionID and MessageSet in the partition
type PartitionResponse struct {
	Partition           int32
	ErrorCode           int16
	HighwaterMarkOffset int64
	lastStableOffset    int64
	logStartOffset      int64
	abortedTransactions []*abortedTransaction
	MessageSetSizeBytes int32
	MessageSet          MessageSet
}

// FetchResponse stores topicname and arrya of PartitionResponse
type FetchResponse struct {
	CorrelationID  int32
	throttleTimeMs int32
	errorCode      int16
	sessionID      int32
	Responses      []struct {
		TopicName          string
		PartitionResponses []PartitionResponse
	}
}

type fetchResponseStreamDecoder struct {
	depositBuffer []byte
	totalLength   int
	//length        int
	buffers  chan []byte
	messages chan *FullMessage
	more     bool

	version uint16

	responsesCount int
	correlationID  int32
}

func (streamDecoder *fetchResponseStreamDecoder) readAll() (length int) {
	length = len(streamDecoder.depositBuffer)
	streamDecoder.depositBuffer = streamDecoder.depositBuffer[:0]

	var buffer []byte
	for {
		buffer = <-streamDecoder.buffers
		if buffer != nil {
			length += len(buffer)
			streamDecoder.more = true
		} else {
			streamDecoder.more = false
			return
		}
	}
}

var errShortRead = errors.New("short read")

func (streamDecoder *fetchResponseStreamDecoder) readToBuf(p []byte) (n int, err error) {
	var (
		l      int
		length int = len(p)
		i      int
	)
	if len(streamDecoder.depositBuffer) >= length {
		l = copy(p, streamDecoder.depositBuffer)
		if l != length {
			return l, errShortRead
		}

		streamDecoder.depositBuffer = streamDecoder.depositBuffer[length:]
		return l, nil
	}

	l = copy(p, streamDecoder.depositBuffer)

	var buffer []byte
	for l < length {
		buffer = <-streamDecoder.buffers
		if buffer != nil {
			streamDecoder.more = true
			i = copy(p[l:], buffer)
			l += i
			//streamDecoder.length += len(buffer)
		} else {
			streamDecoder.more = false
			return l, io.EOF
		}
	}
	streamDecoder.depositBuffer = buffer[i:]

	return length, nil
}

func (streamDecoder *fetchResponseStreamDecoder) read(n int) ([]byte, int) {
	if len(streamDecoder.depositBuffer) >= n {
		rst := streamDecoder.depositBuffer[:n]
		streamDecoder.depositBuffer = streamDecoder.depositBuffer[n:]
		return rst, n
	}

	var (
		rst    []byte = make([]byte, n)
		length int    = 0

		buffer []byte
		i      int
	)

	length = copy(rst, streamDecoder.depositBuffer)

	for length < n {
		buffer = <-streamDecoder.buffers
		if buffer != nil {
			streamDecoder.more = true
			i = copy(rst[length:], buffer)
			length += i
			//streamDecoder.length += len(buffer)
		} else {
			streamDecoder.more = false
			return rst, length
		}
	}
	streamDecoder.depositBuffer = buffer[i:]

	return rst, length
}

func (streamDecoder *fetchResponseStreamDecoder) encodeMessageSet(topicName string, partitionID int32, messageSetSizeBytes int32) error {
	var (
		//messageOffset int64
		messageSize int32
		n           int
		offset      int32 = 0
	)

	hasAtLeastOneMessage := false
	for {
		if offset == messageSetSizeBytes {
			return nil
		}
		buf := make([]byte, 61)
		n, _ = streamDecoder.readToBuf(buf)
		if n < 61 {
			if !hasAtLeastOneMessage {
				return &maxBytesTooSmall
			}
			return nil
		}
		offset += 61

		baseOffset := int64(binary.BigEndian.Uint64(buf))
		// glog.Infof("baseOffset: %d", baseOffset)

		batchLength := binary.BigEndian.Uint32(buf[8:])
		// glog.Infof("batchLength: %d", batchLength)

		// partitionLeaderEpoch := binary.BigEndian.Uint32(buf[12:])
		// glog.Infof("partitionLeaderEpoch: %d", partitionLeaderEpoch)

		// magic := buf[16]
		// glog.Infof("magic: %d", magic)

		// crc := binary.BigEndian.Uint32(buf[17:])
		// glog.Infof("crc: %d", crc)

		attributes := binary.BigEndian.Uint16(buf[21:])
		// glog.Infof("attributes: %d", attributes)

		compress := attributes & 0b11
		// glog.Infof("compress: %d", compress)

		// lastOffsetDelta := binary.BigEndian.Uint32(buf[23:])
		// glog.Infof("lastOffsetDalta: %d", lastOffsetDelta)

		// baseTimestamp := binary.BigEndian.Uint64(buf[27:])
		// glog.Infof("baseTimestamp: %d", baseTimestamp)

		// maxTimestamp := binary.BigEndian.Uint64(buf[35:])
		// glog.Infof("maxTimestamp: %d", maxTimestamp)

		// producerID := binary.BigEndian.Uint64(buf[43:])
		// glog.Infof("producerID: %d", producerID)

		// producerEpoch := binary.BigEndian.Uint16(buf[51:])
		// glog.Infof("producerEpoch: %d", producerEpoch)

		// baseSequence := binary.BigEndian.Uint32(buf[53:])
		// glog.Infof("baseSequence: %d", baseSequence)

		count := int(binary.BigEndian.Uint32(buf[57:]))
		// glog.Infof("count: %d", count)

		if count <= 0 {
			return nil
		}

		buf = make([]byte, batchLength-49)
		if n, err := streamDecoder.readToBuf(buf); err != nil {
			glog.Errorf("readToBuf:%d %s", n, err)
			return nil
		}

		if compress == 3 {
			reader := lz4.NewReader(bytes.NewReader(buf))
			b, err := ioutil.ReadAll(reader)
			if err != nil {
				glog.Infof("decode lz4 error: %v", err)
			}
			buf = b
		}

		offset = 0
		for i := 0; i < count; i++ {
			record, o := DecodeToRecord(buf[offset:])
			// glog.Infof("o: %d, record: %+v", o, record)
			offset += int32(o)
			message := &Message{
				Offset: int64(record.offsetDelta) + baseOffset,
				Key:    record.key,
				Value:  record.value,
			}
			streamDecoder.messages <- &FullMessage{
				TopicName:   topicName,
				PartitionID: partitionID,
				Message:     message,
			}
		}
		return nil

		messageSize = int32(binary.BigEndian.Uint32(buf[8:12]))
		if messageSize <= 0 {
			return nil
		}
		offset += 4

		value := make([]byte, 12+int(messageSize))
		copy(value, buf)
		n, _ = streamDecoder.readToBuf(value[12:])

		if n < int(messageSize) {
			if !hasAtLeastOneMessage {
				return &maxBytesTooSmall
			}
			return nil
		}

		offset += messageSize

		messageSet, err := DecodeToMessageSet(value)

		if err != nil {
			return err
		} else {
			for i := range messageSet {
				streamDecoder.messages <- &FullMessage{
					TopicName:   topicName,
					PartitionID: partitionID,
					Message:     messageSet[i],
				}
			}
		}
		hasAtLeastOneMessage = true
	}
}

func (streamDecoder *fetchResponseStreamDecoder) encodePartitionResponse(topicName string, version uint16) error {
	var (
		partition int32
		errorCode int16
		//highwaterMarkOffset int64
		messageSetSizeBytes int32
		err                 error

		buffer []byte
		n      int
	)

	buffer, n = streamDecoder.read(18 + 20)

	if n < 18 {
		return &maxBytesTooSmall
	}

	partition = int32(binary.BigEndian.Uint32(buffer))

	errorCode = int16(binary.BigEndian.Uint16(buffer[4:]))
	if errorCode != 0 {
		return getErrorFromErrorCode(errorCode)
	}

	//highwaterMarkOffset = int64(binary.BigEndian.Uint64(buffer[6:]))

	messageSetSizeBytes = int32(binary.BigEndian.Uint32((buffer[14+20:])))
	// glog.Infof("messageSetSizeBytes: %d", messageSetSizeBytes)

	err = streamDecoder.encodeMessageSet(topicName, partition, messageSetSizeBytes)
	return err
}

func (streamDecoder *fetchResponseStreamDecoder) encodeResponses(version uint16) error {
	var (
		err    error
		buffer []byte
	)

	buffer, _ = streamDecoder.read(2)
	topicNameLength := int(binary.BigEndian.Uint16(buffer))

	buffer, _ = streamDecoder.read(topicNameLength)
	topicName := string(buffer)

	buffer, _ = streamDecoder.read(4)
	partitionResponseCount := binary.BigEndian.Uint32(buffer)

	if partitionResponseCount == 0 {
		return &noPartitionResponse
	}

	for ; partitionResponseCount > 0; partitionResponseCount-- {
		err = streamDecoder.encodePartitionResponse(topicName, version)
		if err != nil {
			return err
		}
	}
	return nil
}

func (streamDecoder *fetchResponseStreamDecoder) decodeHeader(version uint16) error {
	var (
		headerLength int
		countOffset  int
	)
	switch version {
	case 0:
		headerLength = 8
		countOffset = 4
	case 10:
		headerLength = 18
		countOffset = 14
	}
	buffer, n := streamDecoder.read(headerLength)
	if n != headerLength {
		return fmt.Errorf("could read enough bytes(8) from buffer channel for fetch response header. read %d bytes", n)
	}

	streamDecoder.correlationID = int32(binary.BigEndian.Uint32(buffer))
	if glog.V(10) {
		glog.Infof("fetch correlationID: %d", streamDecoder.correlationID)
	}
	streamDecoder.responsesCount = int(binary.BigEndian.Uint32(buffer[countOffset:]))
	return nil
}

func (streamDecoder *fetchResponseStreamDecoder) streamDecode(version uint16) error {
	defer func() {
		//close(streamDecoder.messages)
		streamDecoder.messages <- nil
	}()

	streamDecoder.depositBuffer = make([]byte, 0)

	payloadLengthBuf, n := streamDecoder.read(4)
	if n != 4 {
		return fmt.Errorf("could not read enough bytes(4) to get fetchresponse length. read %d bytes", n)
	}
	responseLength := binary.BigEndian.Uint32(payloadLengthBuf)
	streamDecoder.totalLength = int(responseLength) + 4

	if err := streamDecoder.decodeHeader(version); err != nil {
		return err
	}
	if streamDecoder.responsesCount == 0 {
		return nil
	}

	for i := 0; i < streamDecoder.responsesCount; i++ {
		err := streamDecoder.encodeResponses(version)
		if err != nil {
			streamDecoder.messages <- &FullMessage{
				TopicName:   "",
				PartitionID: -1,
				Error:       err,
				Message:     nil,
			}
			glog.Error(err)
		}
	}

	// 早期版本的协议可能会有多出来的字节,需要丢弃
	if streamDecoder.more {
		n = streamDecoder.readAll()
	}
	if glog.V(10) {
		glog.Infof("fetch correlationID: %d done", streamDecoder.correlationID)
	}

	return nil
}

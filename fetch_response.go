package healer

import (
	"compress/gzip"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/golang/glog"
	"github.com/pierrec/lz4"
)

var errFetchResponseTooShortNoMagic = errors.New("fetch response too short, could not get magic value")
var errFetchResponseTooShortNoRecordsMeta = errors.New("fetch response too short, could not get records metadata(form baseOffset to baseSequence")

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

func (streamDecoder *fetchResponseStreamDecoder) Read(p []byte) (n int, err error) {
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

// uncompress read all remaining bytes and uncompress them
func (streamDecoder *fetchResponseStreamDecoder) uncompress(compress int8) (uncompressedBytes []byte, err error) {
	switch compress {
	case COMPRESSION_NONE:
		uncompressedBytes, err = ioutil.ReadAll(streamDecoder)
		if err != nil {
			return uncompressedBytes, fmt.Errorf("read uncompressed records bytes error: %w", err)
		}
	case COMPRESSION_GZIP:
		reader, err := gzip.NewReader(streamDecoder)
		if err != nil {
			return nil, fmt.Errorf("create gzip reader of records bytes error: %w", err)
		}
		if uncompressedBytes, err = ioutil.ReadAll(reader); err != nil && err != io.EOF {
			return nil, fmt.Errorf("uncompress gzip records error: %w", err)
		}
	case COMPRESSION_SNAPPY:
		buf, err := ioutil.ReadAll(streamDecoder)
		if err != nil {
			return nil, fmt.Errorf("read streamDecoder error: %w", err)
		}
		uncompressedBytes, err = snappy.Decode(buf)
		if err != nil {
			return nil, fmt.Errorf("uncompress snappy records error: %w", err)
		}
	case COMPRESSION_LZ4:
		reader := lz4.NewReader(streamDecoder)
		uncompressedBytes, err = ioutil.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("uncompress lz4 records error: %w", err)
		}
	}
	return uncompressedBytes, nil
}

func (streamDecoder *fetchResponseStreamDecoder) decodeMessageSetMagic0or1(topicName string, partitionID int32, messageSetSizeBytes int32, header17 []byte) (err error) {
	var (
		messageSize          int32
		offset               int32 = 0
		hasAtLeastOneMessage bool  = false
	)

	defer func() {
		if hasAtLeastOneMessage == false && err == nil {
			err = &maxBytesTooSmall
		}
	}()

	firstMessageSet := true
	var value []byte
	for {
		if offset == messageSetSizeBytes {
			return
		}

		if firstMessageSet {
			firstMessageSet = false
			messageSize = int32(binary.BigEndian.Uint32(header17[8:]))
			value = make([]byte, 12+int(messageSize)) // messageSize doesn't include the size of messageSize itself. 12 equals to the size of the header of offset & message_size.
			copy(value, header17)
			n, _ := streamDecoder.Read(value[17:])
			if n < int(messageSize)-17 {
				return
			}

			offset += messageSize + 12
		} else {
			buf, n := streamDecoder.read(12)
			if n < 12 {
				return
			}
			messageSize = int32(binary.BigEndian.Uint32(buf[8:]))
			value = make([]byte, 12+int(messageSize))
			n, err = streamDecoder.Read(value[12:])
			if err != nil {
				return err
			}
			if n != int(messageSize) {
				return
			}

			offset += messageSize + 12
		}

		messageSet, err := DecodeToMessageSet(value)

		if err != nil {
			return err
		}

		for i := range messageSet {
			streamDecoder.messages <- &FullMessage{
				TopicName:   topicName,
				PartitionID: partitionID,
				Message:     messageSet[i],
			}
			hasAtLeastOneMessage = true
		}
	}
}
func (streamDecoder *fetchResponseStreamDecoder) decodeRecordsMagic2(topicName string, partitionID int32, header17 []byte) (err error) {
	bytesBeforeRecordsLength := 44
	bytesBeforeRecords, n := streamDecoder.read(bytesBeforeRecordsLength)
	if n < bytesBeforeRecordsLength {
		return errFetchResponseTooShortNoRecordsMeta
	}
	buf := make([]byte, len(bytesBeforeRecords)+len(header17))
	copy(buf, header17)
	copy(buf[len(header17):], bytesBeforeRecords)

	baseOffset := int64(binary.BigEndian.Uint64(buf))
	glog.V(15).Infof("baseOffset: %d", baseOffset)
	// batchLength := binary.BigEndian.Uint32(buf[8:])
	// partitionLeaderEpoch := binary.BigEndian.Uint32(buf[12:])
	// magic := buf[16]
	// crc := binary.BigEndian.Uint32(buf[17:])
	attributes := binary.BigEndian.Uint16(buf[21:])
	compress := attributes & 0b11
	glog.V(15).Infof("compress: %d", compress)
	// lastOffsetDelta := binary.BigEndian.Uint32(buf[23:])
	// baseTimestamp := binary.BigEndian.Uint64(buf[27:])
	// maxTimestamp := binary.BigEndian.Uint64(buf[35:])
	// producerID := binary.BigEndian.Uint64(buf[43:])
	// producerEpoch := binary.BigEndian.Uint16(buf[51:])
	// baseSequence := binary.BigEndian.Uint32(buf[53:])

	count := int(binary.BigEndian.Uint32(buf[57:]))
	glog.V(15).Infof("count: %d", count)

	if count <= 0 {
		return nil
	}

	uncompressedBytes, err := streamDecoder.uncompress(int8(compress))
	if err != nil {
		return err
	}
	glog.V(100).Infof("uncompressedBytes: %v", uncompressedBytes)

	offset := 0
	for i := 0; i < count; i++ {
		record, o := DecodeToRecord(uncompressedBytes[offset:])
		glog.V(15).Infof("o: %d, record: %+v", o, record)
		offset += o
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
}

func (streamDecoder *fetchResponseStreamDecoder) decodeMessageSet(topicName string, partitionID int32, messageSetSizeBytes int32, version uint16) (err error) {
	// payload before magic
	header17, n := streamDecoder.read(17)
	if n < 16 {
		return errFetchResponseTooShortNoMagic
	}

	magic := header17[16]
	glog.Infof("magic: %d", magic)
	if magic < 2 {
		return streamDecoder.decodeMessageSetMagic0or1(topicName, partitionID, messageSetSizeBytes, header17)
	}
	return streamDecoder.decodeRecordsMagic2(topicName, partitionID, header17)
}

func (streamDecoder *fetchResponseStreamDecoder) decodePartitionResponse(topicName string, version uint16) error {
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

	if n < 38 {
		return &maxBytesTooSmall
	}
	glog.Infof("buffer: %v", buffer)

	partition = int32(binary.BigEndian.Uint32(buffer))

	errorCode = int16(binary.BigEndian.Uint16(buffer[4:]))
	if errorCode != 0 {
		return getErrorFromErrorCode(errorCode)
	}

	//highwaterMarkOffset = int64(binary.BigEndian.Uint64(buffer[6:]))

	messageSetSizeBytes = int32(binary.BigEndian.Uint32((buffer[34:])))
	glog.Infof("messageSetSizeBytes: %d", messageSetSizeBytes)

	err = streamDecoder.decodeMessageSet(topicName, partition, messageSetSizeBytes, version)
	return err
}

func (streamDecoder *fetchResponseStreamDecoder) decodeResponses(version uint16) error {
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
		err = streamDecoder.decodePartitionResponse(topicName, version)
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
		err := streamDecoder.decodeResponses(version)
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

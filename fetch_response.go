package healer

import (
	"compress/gzip"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	snappy "github.com/eapache/go-xerial-snappy"
	"github.com/pierrec/lz4"
)

var errFetchResponseTooShortNoRecordsMeta = errors.New("fetch response too short, could not get records metadata(form baseOffset to baseSequence)")

type fetchResponseStreamDecoder struct {
	ctx context.Context

	totalLength int
	offset      int

	buffers  io.Reader
	messages chan *FullMessage
	more     bool

	version uint16

	responsesCount int
	correlationID  int32

	startOffset int64

	hasOneMessage bool
}

func (streamDecoder *fetchResponseStreamDecoder) putMessage(msg *FullMessage) error {
	select {
	case <-streamDecoder.ctx.Done():
		return streamDecoder.ctx.Err()
	case streamDecoder.messages <- msg:
		return nil
	}
}
func (streamDecoder *fetchResponseStreamDecoder) readAll() (length int, err error) {
	defer func() {
		streamDecoder.offset += length
	}()

	buf := make([]byte, streamDecoder.totalLength-streamDecoder.offset)
	return io.ReadFull(streamDecoder.buffers, buf)
}

var errShortRead = errors.New("short read")

func (streamDecoder *fetchResponseStreamDecoder) Read(p []byte) (n int, err error) {
	defer func() {
		streamDecoder.offset += n
	}()

	return streamDecoder.buffers.Read(p)
}

// read util getting n bytes
func (streamDecoder *fetchResponseStreamDecoder) read(n int) (rst []byte, length int, err error) {
	defer func() {
		streamDecoder.offset += length
	}()

	rst = make([]byte, n)
	length, err = io.ReadFull(streamDecoder.buffers, rst)

	return rst, length, err
}

// uncompress read all remaining bytes and uncompress them
func uncompress(compress int8, reader io.Reader) (uncompressedBytes []byte, err error) {
	switch compress {
	case COMPRESSION_NONE:
		uncompressedBytes, err = io.ReadAll(reader)
		if err != nil {
			return uncompressedBytes, fmt.Errorf("read uncompressed records bytes error: %w", err)
		}
	case COMPRESSION_GZIP:
		reader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("create gzip reader of records bytes error: %w", err)
		}
		if uncompressedBytes, err = io.ReadAll(reader); err != nil && err != io.EOF {
			return nil, fmt.Errorf("uncompress gzip records error: %w", err)
		}
	case COMPRESSION_SNAPPY:
		buf, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("read streamDecoder error: %w", err)
		}
		uncompressedBytes, err = snappy.Decode(buf)
		if err != nil {
			return nil, fmt.Errorf("uncompress snappy records error: %w", err)
		}
	case COMPRESSION_LZ4:
		reader := lz4.NewReader(reader)
		uncompressedBytes, err = io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("uncompress lz4 records error: %w", err)
		}
	}
	return uncompressedBytes, nil
}

func (streamDecoder *fetchResponseStreamDecoder) decodeMessageSetMagic0or1(topicName string, partitionID int32, magic int, header17 []byte) (offset int, err error) {
	firstMessageSet := true
	var value []byte
	for {
		if firstMessageSet {
			firstMessageSet = false
			messageSize := int(binary.BigEndian.Uint32(header17[8:]))
			value = make([]byte, 12+messageSize) // messageSize doesn't include the size of messageSize itself. 12 equals to the size of the header of offset & message_size.
			copy(value, header17)
			n, e := streamDecoder.Read(value[17:])
			if e != nil {
				return offset, e
			}
			if n < messageSize-17 {
				return
			}

			offset += messageSize + 12
		} else {
			buf, n, err := streamDecoder.read(12)
			if err != nil {
				return offset, err
			}
			if n < 12 {
				return offset, err
			}
			messageSize := int(binary.BigEndian.Uint32(buf[8:]))
			value = make([]byte, 12+messageSize)
			n, err = streamDecoder.Read(value[12:])
			if err != nil {
				return offset, err
			}
			if n < messageSize {
				return offset, err
			}
			copy(value, buf)

			offset += messageSize + 12
		}

		messageSet, err := DecodeToMessageSet(value)

		if err != nil {
			return offset, err
		}

		if len(messageSet) == 0 {
			return offset, nil
		}

		// TODO send each message to the channel directly?
		for i := range messageSet {
			if messageSet[i].Offset >= streamDecoder.startOffset {
				msg := &FullMessage{
					TopicName:   topicName,
					PartitionID: partitionID,
					Message:     messageSet[i],
				}
				if err = streamDecoder.putMessage(msg); err != nil {
					return offset, err
				}
				streamDecoder.hasOneMessage = true
			} else {
				logger.Info("offset smaller than startOffset", "offset", "topic", topicName, "partition", partitionID, messageSet[i].Offset, "startOffset", streamDecoder.startOffset)
			}
		}
	}
}

// offset returned equals to batchLength + 12
func (streamDecoder *fetchResponseStreamDecoder) decodeRecordsMagic2(topicName string, partitionID int32, header17 []byte) (offset int, err error) {
	bytesBeforeRecordsLength := 44 // (magic, records count]
	bytesBeforeRecords, n, err := streamDecoder.read(bytesBeforeRecordsLength)
	if err != nil {
		return offset, err
	}

	offset += n
	if n < bytesBeforeRecordsLength {
		return offset, errFetchResponseTooShortNoRecordsMeta
	}
	buf := make([]byte, len(bytesBeforeRecords)+len(header17))
	copy(buf, header17)
	copy(buf[len(header17):], bytesBeforeRecords)

	baseOffset := int64(binary.BigEndian.Uint64(buf))
	batchLength := binary.BigEndian.Uint32(buf[8:])
	// partitionLeaderEpoch := binary.BigEndian.Uint32(buf[12:])
	// magic := buf[16]
	// crc := binary.BigEndian.Uint32(buf[17:])
	attributes := binary.BigEndian.Uint16(buf[21:])
	compress := attributes & 0b11
	// lastOffsetDelta := binary.BigEndian.Uint32(buf[23:])
	baseTimestamp := binary.BigEndian.Uint64(buf[27:])
	// maxTimestamp := binary.BigEndian.Uint64(buf[35:])
	// producerID := binary.BigEndian.Uint64(buf[43:])
	// producerEpoch := binary.BigEndian.Uint16(buf[51:])
	// baseSequence := binary.BigEndian.Uint32(buf[53:])

	// count is not accurate, payload maybe truncated by maxsize parameter in fetch request
	count := int(binary.BigEndian.Uint32(buf[57:]))

	if count <= 0 {
		return
	}

	// 49 is length of (batchLength, records count]
	if int(batchLength)-49 > streamDecoder.totalLength-streamDecoder.offset {
		n, err = streamDecoder.readAll()
		offset += n
		return offset, err
	}

	r := io.LimitReader(streamDecoder, int64(batchLength)-49)
	uncompressedBytes, err := uncompress(int8(compress), r)
	if err != nil {
		return offset, fmt.Errorf("uncompress records bytes error: %w", err)
	}
	offset += int(batchLength) - 49

	uncompressedBytesOffset := 0
	for i := 0; i < count; i++ {
		record, o, err := DecodeToRecord(uncompressedBytes[uncompressedBytesOffset:])
		if err != nil {
			if err == errUncompleteRecord {
				err = nil
			}
			return offset, err
		}
		uncompressedBytesOffset += o
		message := &Message{
			Offset:     int64(record.offsetDelta) + baseOffset,
			Timestamp:  uint64(record.timestampDelta) + baseTimestamp,
			Attributes: record.attributes,
			MagicByte:  2,
			Key:        record.key,
			Value:      record.value,
		}
		if message.Offset >= streamDecoder.startOffset {
			msg := &FullMessage{
				TopicName:   topicName,
				PartitionID: partitionID,
				Message:     message,
			}
			if err = streamDecoder.putMessage(msg); err != nil {
				return offset, err
			}
			streamDecoder.hasOneMessage = true
		}
	}

	return offset, nil
}

// messageSetSizeBytes may include more the one `Record Batch`, that is, `Record Batch`,`Record Batch`,`Record Batch`...
func (streamDecoder *fetchResponseStreamDecoder) decodeMessageSet(topicName string, partitionID int32, messageSetSizeBytes int32, version uint16) (err error) {
	defer func() {
		if err == io.EOF || err == &maxBytesTooSmall {
			if streamDecoder.hasOneMessage {
				err = nil
			}
		}
		if !streamDecoder.hasOneMessage && err == nil {
			err = &maxBytesTooSmall
		}
	}()

	var (
		offset int
		o      int
	)
	for offset < int(messageSetSizeBytes) {
		// payload before magic
		header17, n, e := streamDecoder.read(17)
		if e != nil {
			return e
		}
		if n < 17 {
			return
		}
		offset += n

		magic := header17[16]

		if magic < 2 {
			o, err = streamDecoder.decodeMessageSetMagic0or1(topicName, partitionID, int(magic), header17)
		} else {
			o, err = streamDecoder.decodeRecordsMagic2(topicName, partitionID, header17)
		}
		offset += o
		if err != nil {
			// if err == errUncompleteRecord {
			// 	hasAtLeastOneMessage = true
			// 	return nil
			// }
			return err
		}
	}
	return nil
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

	var bytesBeforeRecordsLength int // (partition_index, messageSetSizeBytes]
	switch version {
	case 0:
		bytesBeforeRecordsLength = 18
	case 10:
		bytesBeforeRecordsLength = 38
	}
	buffer, n, err = streamDecoder.read(bytesBeforeRecordsLength)
	if err != nil {
		return err
	}

	if n < bytesBeforeRecordsLength {
		return &maxBytesTooSmall
	}

	partition = int32(binary.BigEndian.Uint32(buffer))

	errorCode = int16(binary.BigEndian.Uint16(buffer[4:]))
	if errorCode != 0 {
		return KafkaError(errorCode)
	}

	messageSetSizeBytes = int32(binary.BigEndian.Uint32((buffer[bytesBeforeRecordsLength-4:])))
	if messageSetSizeBytes == 0 {
		return nil
	}

	// if we use fetch request with version 0 and not big enough fetch.max.bytes, kafka server may return partial records, and the NOT begins with the requests offset.
	// healer will ignore the records, double fetch.max.bytes, and then retry.
	if int(messageSetSizeBytes) > streamDecoder.totalLength-streamDecoder.offset {
		return &maxBytesTooSmall
	}

	err = streamDecoder.decodeMessageSet(topicName, partition, messageSetSizeBytes, version)
	return err
}

func (streamDecoder *fetchResponseStreamDecoder) decodeResponses(version uint16) error {
	var (
		err    error
		buffer []byte
	)

	buffer, _, err = streamDecoder.read(2)
	if err != nil {
		return err
	}
	topicNameLength := int(binary.BigEndian.Uint16(buffer))

	buffer, _, err = streamDecoder.read(topicNameLength)
	if err != nil {
		return err
	}
	topicName := string(buffer)

	buffer, _, err = streamDecoder.read(4)
	if err != nil {
		return err
	}
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
	buffer, n, err := streamDecoder.read(headerLength)
	if err != nil {
		return err
	}
	if n != headerLength {
		return fmt.Errorf("could read enough bytes of fetch response header. read %d bytes", n)
	}

	streamDecoder.correlationID = int32(binary.BigEndian.Uint32(buffer))
	streamDecoder.responsesCount = int(binary.BigEndian.Uint32(buffer[countOffset:]))
	return nil
}

func (streamDecoder *fetchResponseStreamDecoder) streamDecode(ctx context.Context, startOffset int64) error {
	streamDecoder.offset = 0
	streamDecoder.startOffset = startOffset
	streamDecoder.hasOneMessage = false

	if err := streamDecoder.decodeHeader(streamDecoder.version); err != nil {
		return err
	}
	if streamDecoder.responsesCount == 0 {
		return nil
	}

	for i := 0; i < streamDecoder.responsesCount; i++ {
		err := streamDecoder.decodeResponses(streamDecoder.version)
		if err != nil {
			msg := &FullMessage{
				TopicName:   "",
				PartitionID: -1,
				Error:       err,
				Message:     nil,
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-streamDecoder.ctx.Done():
				return streamDecoder.ctx.Err()
			case streamDecoder.messages <- msg:
				return nil
			}
		}
	}

	// 早期版本的协议可能会有多出来的字节,需要丢弃
	if streamDecoder.more {
		streamDecoder.readAll()
	}
	logger.V(5).Info("decode fetch response done", "correlationID", streamDecoder.correlationID)

	return nil
}

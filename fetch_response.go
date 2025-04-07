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

type RecordBatch struct {
	BaseOffset           int64
	BatchLength          int32
	PartitionLeaderEpoch int32
	Magic                int8
	CRC                  uint32
	Attributes           int16
	LastOffsetDelta      int32
	BaseTimestamp        int64
	MaxTimestamp         int64
	ProducerID           int64
	ProducerEpoch        int16
	BaseSequence         int32
	Records              []Record
}
type PartitionResponse struct {
	PartitionID         int32
	ErrorCode           int16
	HighWatermark       int64
	LastStableOffset    int64
	LogStartOffset      int64
	AbortedTransactions []struct {
		ProducerID  int64
		FirstOffset int64
	}
	RecordBatchesLength int32
	RecordBatches       []RecordBatch
}

type FetchResponse struct {
	CorrelationID  int32
	ThrottleTimeMs int32
	ErrorCode      int16
	SessionID      int32
	Responses      map[string][]PartitionResponse
}

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

func (streamDecoder *fetchResponseStreamDecoder) readAll() (length int, err error) {
	defer func() {
		streamDecoder.offset += length
	}()

	written, err := io.Copy(io.Discard, streamDecoder.buffers)
	return int(written), err
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
	switch CompressType(compress) {
	case CompressionNone:
		uncompressedBytes, err = io.ReadAll(reader)
		if err != nil {
			return uncompressedBytes, fmt.Errorf("read uncompressed records bytes error: %w", err)
		}
	case CompressionGzip:
		reader, err := gzip.NewReader(reader)
		if err != nil {
			return nil, fmt.Errorf("create gzip reader of records bytes error: %w", err)
		}
		if uncompressedBytes, err = io.ReadAll(reader); err != nil && err != io.EOF {
			return nil, fmt.Errorf("uncompress gzip records error: %w", err)
		}
	case CompressionSnappy:
		buf, err := io.ReadAll(reader)
		if err != nil {
			return nil, fmt.Errorf("read streamDecoder error: %w", err)
		}
		uncompressedBytes, err = snappy.Decode(buf)
		if err != nil {
			return nil, fmt.Errorf("uncompress snappy records error: %w", err)
		}
	case CompressionLz4:
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
	// value is the bytes of whole record consists of (offset(int64) message_size(int32) message)
	var value []byte
	for {
		if firstMessageSet {
			firstMessageSet = false

			// messageSize doesn't include the size of messageSize itself.
			messageSize := int(binary.BigEndian.Uint32(header17[8:]))
			if messageSize < 6 {
				return
			}

			// 12 is the size of offset(int64) & message_size(int32) in header17
			value = make([]byte, 12+messageSize)
			n, e := streamDecoder.Read(value[17:])
			offset += n
			if e != nil {
				return offset, e
			}

			// remaining bytes is incomplete
			if n < len(value)-17 {
				return offset, err
			}
			copy(value, header17)
		} else {
			buf, n, err := streamDecoder.read(12)
			offset += n
			if err != nil {
				return offset, err
			}
			if n < 12 {
				return offset, err
			}
			messageSize := int(binary.BigEndian.Uint32(buf[8:]))
			value = make([]byte, 12+messageSize)
			n, err = streamDecoder.Read(value[12:])
			offset += n
			if err != nil {
				return offset, err
			}

			// remaining bytes is incomplete
			if n < len(value)-17 {
				return offset, err
			}
			copy(value, buf)
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
			if err = streamDecoder.filterAndPutMessage(messageSet[i], topicName, partitionID); err != nil {
				return offset, err
			}
		}
	}
}

// offset returned equals to batchLength + 12
func (streamDecoder *fetchResponseStreamDecoder) decodeRecordsMagic2(topicName string, partitionID int32, header17 []byte) (offset int, err error) {
	bytesBeforeRecordsLength := 44 // (magic, records count]
	bytesBeforeRecords, n, err := streamDecoder.read(bytesBeforeRecordsLength)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			return n, nil
		}
		return n, err
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
	// logger.Info("record batch info",
	// 	"baseOffset", baseOffset, "batchLength", batchLength, "partitionLeaderEpoch", partitionLeaderEpoch,
	// 	"magic", magic, "crc", crc, "attributes", attributes, "compress", compress, "lastOffsetDelta", lastOffsetDelta,
	// 	"baseTimestamp", baseTimestamp, "maxTimestamp", maxTimestamp, "producerID", producerID, "producerEpoch",
	// 	producerEpoch, "baseSequence", baseSequence, "count", count)

	if count <= 0 {
		return
	}

	r := io.LimitReader(streamDecoder, int64(batchLength)-49)
	// set offset immediately because we have to skip the data even if uncompress error
	offset += int(batchLength) - 49
	uncompressedBytes, err := uncompress(int8(compress), r)
	if err != nil {
		return offset, fmt.Errorf("uncompress records bytes error: %w", err)
	}

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
			Headers:    record.Headers,
		}
		if err = streamDecoder.filterAndPutMessage(message, topicName, partitionID); err != nil {
			return offset, err
		}
	}

	return offset, nil
}

func (streamDecoder *fetchResponseStreamDecoder) filterAndPutMessage(message *Message, topicName string, partitionID int32) (err error) {
	if streamDecoder.filterMessage(message) {
		msg := &FullMessage{
			TopicName:   topicName,
			PartitionID: partitionID,
			Message:     message,
		}
		if err = streamDecoder.putMessage(msg); err != nil {
			return err
		}
		streamDecoder.hasOneMessage = true
	} else {
		logger.Info("offset smaller than startOffset", "offset", message.Offset, "topic", topicName, "partition", partitionID, "startOffset", streamDecoder.startOffset)
	}
	return nil
}

func (streamDecoder *fetchResponseStreamDecoder) filterMessage(message *Message) bool {
	return message.Offset >= streamDecoder.startOffset
}

func (streamDecoder *fetchResponseStreamDecoder) putMessage(msg *FullMessage) error {
	select {
	case <-streamDecoder.ctx.Done():
		return streamDecoder.ctx.Err()
	case streamDecoder.messages <- msg:
		return nil
	}
}

// messageSetSizeBytes may include more the one `Record Batch`,
// that is, `Record Batch`,`Record Batch`,`Record Batch`...
func (streamDecoder *fetchResponseStreamDecoder) decodeMessageSet(topicName string, partitionID int32, messageSetSizeBytes int32, version uint16) (err error) {
	defer func() {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || err == &maxBytesTooSmall {
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
		// payload before magic: [BaseOffset: Magic]
		header17, n, e := streamDecoder.read(17)
		if e != nil {
			if e == io.ErrUnexpectedEOF {
				return nil
			}
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

func (streamDecoder *fetchResponseStreamDecoder) decodePartitionResponse(topicName string, version uint16) (err error) {
	var (
		p   PartitionResponse
		buf []byte = make([]byte, 8)
	)

	defer func() {
		if errors.Is(err, io.EOF) {
			err = &maxBytesTooSmall
		}
	}()

	if _, err = streamDecoder.Read(buf[:4]); err != nil {
		return err
	}
	p.PartitionID = int32(binary.BigEndian.Uint32(buf))

	if _, err = streamDecoder.Read(buf[:2]); err != nil {
		return err
	}
	p.ErrorCode = int16(binary.BigEndian.Uint16(buf))
	if p.ErrorCode != 0 {
		return KafkaError(p.ErrorCode)
	}

	if _, err = streamDecoder.Read(buf[:8]); err != nil {
		return err
	}
	// p.HighWatermark = int64(binary.BigEndian.Uint64(buf))

	switch version {
	case 7, 10:
		if _, err = streamDecoder.Read(buf[:8]); err != nil {
			return err
		}
		// p.LastStableOffset = int64(binary.BigEndian.Uint64(buf))

		if _, err = streamDecoder.Read(buf[:8]); err != nil {
			return err
		}
		// p.LogStartOffset = int64(binary.BigEndian.Uint64(buf))

		if _, err = streamDecoder.Read(buf[:4]); err != nil {
			return err
		}
		abortedTransactionsCount := int32(binary.BigEndian.Uint32(buf))
		if abortedTransactionsCount > 0 {
			p.AbortedTransactions = make([]struct {
				ProducerID  int64
				FirstOffset int64
			}, abortedTransactionsCount)
			for i := 0; i < int(abortedTransactionsCount); i++ {
				if _, err = streamDecoder.Read(buf[:8]); err != nil {
					return err
				}
				p.AbortedTransactions[i].ProducerID = int64(binary.BigEndian.Uint64(buf))

				if _, err = streamDecoder.Read(buf[:8]); err != nil {
					return err
				}
				p.AbortedTransactions[i].FirstOffset = int64(binary.BigEndian.Uint64(buf))
			}
		}
	}

	if _, err = streamDecoder.Read(buf[:4]); err != nil {
		return err
	}
	p.RecordBatchesLength = int32(binary.BigEndian.Uint32(buf))
	if p.RecordBatchesLength <= 0 {
		return nil
	}

	// RecordBatchLength consists of more than one Record, so we try to decode the messageSet
	// if int(p.RecordBatchLength) > streamDecoder.totalLength-streamDecoder.offset { }

	err = streamDecoder.decodeMessageSet(topicName, p.PartitionID, p.RecordBatchesLength, version)
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
	case 7, 10:
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

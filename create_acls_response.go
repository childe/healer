package healer

import (
	"encoding/binary"
	"fmt"
)

type CreateAclsResponse struct {
	ResponseHeader
	Results      []AclCreationResult
	TaggedFields TaggedFields
}

type AclCreationResult struct {
	ErrorCode    uint16
	ErrorMessage *string
	TaggedFields TaggedFields
}

func (r *CreateAclsResponse) Error() error {
	for _, result := range r.Results {
		if result.ErrorCode != 0 {
			return KafkaError(result.ErrorCode)
		}
	}
	return nil
}

func DecodeCreateAclsResponse(payload []byte, version uint16) (*CreateAclsResponse, error) {
	var (
		r          = &CreateAclsResponse{}
		offset int = 0
		o      int
	)

	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("CreateAclsResponse length did not match: %d!=%d", responseLength+4, len(payload))
	}

	r.ResponseHeader, o = DecodeResponseHeader(payload, API_CreateAcls, version)
	offset += o

	var resultCount int32
	if r.ResponseHeader.IsFlexible() {
		resultCount, o = compactArrayLength(payload[offset:])
		offset += o
	} else {
		resultCount = int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
	}

	if resultCount < 0 {
		r.Results = nil
	} else {
		r.Results = make([]AclCreationResult, resultCount)
		for i := int32(0); i < resultCount; i++ {
			result := &AclCreationResult{}
			result.ErrorCode = binary.BigEndian.Uint16(payload[offset:])
			offset += 2

			if r.ResponseHeader.IsFlexible() {
				result.ErrorMessage, o = compactNullableString(payload[offset:])
				offset += o
			} else {
				result.ErrorMessage, o = nullableString(payload[offset:])
				offset += o
			}
			if r.ResponseHeader.IsFlexible() {
				result.TaggedFields, o = DecodeTaggedFields(payload[offset:])
				offset += o
			}
			r.Results[i] = *result
		}
	}

	if r.ResponseHeader.IsFlexible() {
		r.TaggedFields, o = DecodeTaggedFields(payload[offset:])
		offset += o
	}

	return r, nil
}

func (r *CreateAclsResponse) length() (n int) {
	n += r.ResponseHeader.length()
	n += 4 // results count
	for _, result := range r.Results {
		n += 2 // error code
		n += 2
		if result.ErrorMessage != nil {
			n += len(*result.ErrorMessage) + 2
		}
	}
	return n
}

// just for test
func (r *CreateAclsResponse) Encode() (payload []byte) {
	payload = make([]byte, r.length())
	offset := 0

	binary.BigEndian.PutUint32(payload[offset:], 0) //length
	offset += 4
	defer func() {
		binary.BigEndian.PutUint32(payload, uint32(offset-4))
	}()

	offset += r.ResponseHeader.EncodeTo(payload[offset:])
	if r.ResponseHeader.IsFlexible() {
		offset += copy(payload[offset:], encodeCompactArrayLength(len(r.Results)))
	} else {
		binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Results)))
		offset += 4
	}

	for _, result := range r.Results {
		binary.BigEndian.PutUint16(payload[offset:], result.ErrorCode)
		offset += 2

		if r.ResponseHeader.IsFlexible() {
			offset += copy(payload[offset:], encodeCompactNullableString(result.ErrorMessage))
		} else {
			offset += copy(payload[offset:], encodeNullableString(result.ErrorMessage))
		}

		if r.ResponseHeader.IsFlexible() {
			offset += r.TaggedFields.EncodeTo(payload[offset:])
		}
	}

	if r.ResponseHeader.IsFlexible() {
		offset += copy(payload[offset:], r.TaggedFields.Encode())
	}

	return payload
}

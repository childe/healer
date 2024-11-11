package healer

import (
	"encoding/binary"
	"fmt"
)

type DeleteAclsFilter struct {
	ResourceType   AclsResourceType
	ResourceName   *string
	PatternType    AclsPatternType
	Principal      *string
	Host           *string
	Operation      AclsOperation
	PermissionType AclsPermissionType
	TaggedFields   TaggedFields
}

func (f *DeleteAclsFilter) length() (n int) {
	n += 1 // resource type filter
	n += 2
	if f.ResourceName != nil {
		n += len(*f.ResourceName)
	}
	n += 1 // pattern type filter
	n += 2
	if f.Principal != nil {
		n += len(*f.Principal)
	}
	n += 2
	if f.Host != nil {
		n += len(*f.Host)
	}
	n += 1 // operation

	n += f.TaggedFields.length()
	return
}
func (f *DeleteAclsFilter) EncodeTo(payload []byte, version uint16, isFlexible bool) (offset int) {
	payload[offset] = uint8(f.ResourceType)
	offset++

	if isFlexible {
		offset += copy(payload[offset:], encodeCompactNullableString(f.ResourceName))
	} else {
		offset += copy(payload[offset:], encodeNullableString(f.ResourceName))
	}

	if version >= 1 {
		payload[offset] = uint8(f.PatternType)
		offset++
	}

	if isFlexible {
		offset += copy(payload[offset:], encodeCompactNullableString(f.Principal))
		offset += copy(payload[offset:], encodeCompactNullableString(f.Host))
	} else {
		offset += copy(payload[offset:], encodeNullableString(f.Principal))
		offset += copy(payload[offset:], encodeNullableString(f.Host))
	}

	payload[offset] = uint8(f.Operation)
	offset++

	payload[offset] = uint8(f.PermissionType)
	offset++

	if isFlexible {
		offset += copy(payload[offset:], f.TaggedFields.Encode())
	}
	return offset
}

type DeleteAclsRequest struct {
	RequestHeader
	Filters      []*DeleteAclsFilter
	TaggedFields TaggedFields
}

func NewDeleteAclsRequest(clientID string, filters []*DeleteAclsFilter) *DeleteAclsRequest {
	requestHeader := RequestHeader{
		APIKey:   API_DeleteAcls,
		ClientID: &clientID,
	}

	return &DeleteAclsRequest{
		RequestHeader: requestHeader,
		Filters:       filters,
	}
}

// not exact but enough
func (r *DeleteAclsRequest) length() (n int) {
	n = 4 // length
	n += r.RequestHeader.length()
	n += 4 // filters array length
	for _, filter := range r.Filters {
		n += filter.length()
	}
	return n
}

func (r *DeleteAclsRequest) Encode(version uint16) (payload []byte) {
	payload = make([]byte, r.length())
	offset := 0

	// length
	binary.BigEndian.PutUint32(payload, 0)
	offset += 4
	defer func() {
		binary.BigEndian.PutUint32(payload, uint32(offset-4))
	}()

	offset += r.RequestHeader.EncodeTo(payload[offset:])

	if r.RequestHeader.IsFlexible() {
		offset += copy(payload[offset:], encodeCompactArrayLength(len(r.Filters)))
	} else {
		binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Filters)))
		offset += 4
	}

	for _, f := range r.Filters {
		offset += f.EncodeTo(payload[offset:], r.RequestHeader.APIVersion, r.RequestHeader.IsFlexible())
	}

	if r.RequestHeader.IsFlexible() {
		offset += copy(payload[offset:], r.TaggedFields.Encode())
	}

	return payload[:offset]
}

// just for test
func DecodeDeleteAclsRequest(payload []byte) (*DeleteAclsRequest, error) {
	r := &DeleteAclsRequest{}
	offset := 0
	var o int

	requestLength := int32(binary.BigEndian.Uint32(payload))
	offset += 4
	if requestLength != int32(len(payload)-4) {
		return nil, fmt.Errorf("request length did not match: %d!=%d", requestLength, len(payload)-4)
	}

	r.RequestHeader, o = DecodeRequestHeader(payload[offset:])
	offset += o

	var filterCount int32
	if r.RequestHeader.IsFlexible() {
		filterCount, o = compactArrayLength(payload[offset:])
		offset += o
	} else {
		filterCount = int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
	}

	if filterCount < 0 {
		r.Filters = nil
	} else {
		r.Filters = make([]*DeleteAclsFilter, filterCount)
	}
	for i := 0; i < int(filterCount); i++ {
		f := &DeleteAclsFilter{}
		f.ResourceType = AclsResourceType(payload[offset])
		offset++

		if r.RequestHeader.IsFlexible() {
			f.ResourceName, o = compactNullableString(payload[offset:])
		} else {
			f.ResourceName, o = nullableString(payload[offset:])
		}
		offset += o

		if r.RequestHeader.APIVersion >= 1 {
			f.PatternType = AclsPatternType(payload[offset])
			offset++
		}

		if r.RequestHeader.IsFlexible() {
			f.Principal, o = compactNullableString(payload[offset:])
			offset += o
			f.Host, o = compactNullableString(payload[offset:])
			offset += o
		} else {
			f.Principal, o = nullableString(payload[offset:])
			offset += o
			f.Host, o = nullableString(payload[offset:])
			offset += o
		}

		f.Operation = AclsOperation(payload[offset])
		offset++

		f.PermissionType = AclsPermissionType(payload[offset])
		offset++

		f.TaggedFields, o = DecodeTaggedFields(payload[offset:])
		offset += o

		r.Filters[i] = f
	}

	return r, nil
}

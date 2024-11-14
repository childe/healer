package healer

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
)

// version0
type ListGroupsRequest struct {
	*RequestHeader
	StatesFilter []string `healer:"minVersion:4"`
	TypesFilter  []string `healer:"minVersion:5"`
	TaggedFields TaggedFields
}

var tagCacheListGroupsRequest atomic.Value

func NewListGroupsRequest(clientID string) *ListGroupsRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_ListGroups,
		APIVersion: 0,
		ClientID:   &clientID,
	}
	return &ListGroupsRequest{
		RequestHeader: requestHeader,
		StatesFilter:  make([]string, 0),
		TypesFilter:   make([]string, 0),
	}
}

func (r *ListGroupsRequest) SetStatesFilter(statesFilter []string) {
	r.StatesFilter = statesFilter
}
func (r *ListGroupsRequest) SetTypesFilter(typesFilter []string) {
	r.TypesFilter = typesFilter
}

func (r *ListGroupsRequest) length() (n int) {
	n = r.RequestHeader.length()

	n += 4
	for _, v := range r.StatesFilter {
		n += 2 + len(v)
	}

	n += 4
	for _, v := range r.TypesFilter {
		n += 2 + len(v)
	}

	n += r.TaggedFields.length()

	n += 4 // payload length

	return
}

func (r *ListGroupsRequest) tags() (fieldsVersions map[string]uint16) {
	if v := tagCacheListGroupsRequest.Load(); v != nil {
		return v.(map[string]uint16)
	}

	fieldsVersions = healerTags(*r)
	tagCacheListGroupsRequest.Store(fieldsVersions)
	return
}

func (r *ListGroupsRequest) Encode(version uint16) (payload []byte) {
	tags := r.tags()

	payload = make([]byte, r.length())
	offset := 4 // payload length
	var compactArrayCount int

	defer func() {
		binary.BigEndian.PutUint32(payload, uint32(offset-4))
	}()

	offset += r.RequestHeader.EncodeTo(payload[offset:])

	if r.APIVersion >= tags["StatesFilter"] {
		if r.StatesFilter == nil {
			compactArrayCount = -1
		} else {
			compactArrayCount = len(r.StatesFilter)
		}
		if r.IsFlexible() {
			offset += copy(payload[offset:], encodeCompactArrayLength(compactArrayCount))
		} else {
			binary.BigEndian.PutUint32(payload[offset:], uint32(compactArrayCount))
			offset += 4
		}

		for _, v := range r.StatesFilter {
			if r.IsFlexible() {
				offset += copy(payload[offset:], encodeCompactString(v))
			} else {
				offset += copy(payload[offset:], encodeString(v))
			}
		}
	}

	if r.APIVersion >= tags["TypesFilter"] {
		if r.TypesFilter == nil {
			compactArrayCount = -1
		} else {
			compactArrayCount = len(r.TypesFilter)
		}
		if r.IsFlexible() {
			offset += copy(payload[offset:], encodeCompactArrayLength(compactArrayCount))
		} else {
			binary.BigEndian.PutUint32(payload[offset:], uint32(compactArrayCount))
			offset += 4
		}
		for _, v := range r.TypesFilter {
			if r.IsFlexible() {
				offset += copy(payload[offset:], encodeCompactString(v))
			} else {
				offset += copy(payload[offset:], encodeString(v))
			}
		}
	}
	if r.IsFlexible() {
		offset += r.TaggedFields.EncodeTo(payload[offset:])
	}

	return payload[:offset]
}

// just for test
func DecodeListGroupsRequest(payload []byte) (r *ListGroupsRequest, err error) {
	r = &ListGroupsRequest{}
	offset := 0
	var o int
	tags := r.tags()

	requestLength := int(binary.BigEndian.Uint32(payload))
	offset += 4

	if requestLength != len(payload)-4 {
		return nil, fmt.Errorf("request length did not match actual size")
	}

	header, o := DecodeRequestHeader(payload[offset:])
	r.RequestHeader = &header
	offset += o

	if r.APIVersion >= tags[`StatesFilter`] {
		if r.IsFlexible() {
			l, o := compactArrayLength(payload[offset:])
			offset += o
			if l >= 0 {
				r.StatesFilter = make([]string, l)
				for i := int32(0); i < l; i++ {
					r.StatesFilter[i], o = compactString(payload[offset:])
					offset += o
				}
			}
		} else {
			l := int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			if l >= 0 {
				r.StatesFilter = make([]string, l)
				for i := int32(0); i < l; i++ {
					r.StatesFilter[i], o = nonnullableString(payload[offset:])
					offset += o
				}
			}
		}
	}

	if r.APIVersion >= tags[`TypesFilter`] {
		if r.IsFlexible() {
			l, o := compactArrayLength(payload[offset:])
			offset += o
			if l >= 0 {
				r.TypesFilter = make([]string, l)
				for i := int32(0); i < l; i++ {
					r.TypesFilter[i], o = compactString(payload[offset:])
					offset += o
				}
			}
		} else {
			l := int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			if l >= 0 {
				r.TypesFilter = make([]string, l)
				for i := int32(0); i < l; i++ {
					r.TypesFilter[i], o = nonnullableString(payload[offset:])
					offset += o
				}
			}
		}
	}

	if r.IsFlexible() {
		r.TaggedFields, o = DecodeTaggedFields(payload[offset:])
		offset += o
	}

	return r, nil
}

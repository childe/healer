package healer

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
)

var tagCacheListGroupsResponse atomic.Value

type Group struct {
	GroupID      string
	ProtocolType string
	GroupState   string `healer:"minVersion:4"`
	GroupType    string `healer:"minVersion:5"`
	TaggedFields TaggedFields
}
type ListGroupsResponse struct {
	ResponseHeader
	ThrottleTimeMS int32 `healer:"minVersion:1"`
	ErrorCode      uint16
	Groups         []*Group
	TaggedFields   TaggedFields
}

func (r *ListGroupsResponse) Error() error {
	return nil
}

func (r *ListGroupsResponse) tags() (fieldsVersions map[string]uint16) {
	if v := tagCacheListGroupsResponse.Load(); v != nil {
		return v.(map[string]uint16)
	}

	fieldsVersions = healerTags(*r)
	tagCacheListGroupsResponse.Store(fieldsVersions)
	return
}

func NewListGroupsResponse(payload []byte, version uint16) (r *ListGroupsResponse, err error) {
	r = &ListGroupsResponse{}
	offset := 0
	o := 0
	tags := r.tags()

	responseLength := int(binary.BigEndian.Uint32(payload))
	if responseLength+4 != len(payload) {
		return r, fmt.Errorf("ListGroups response length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.ResponseHeader, o = DecodeResponseHeader(payload[offset:], API_ListGroups, version)
	offset += o

	if version >= tags["ThrottleTimeMS"] {
		r.ThrottleTimeMS = int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
	}

	r.ErrorCode = binary.BigEndian.Uint16(payload[offset:])
	offset += 2

	var groupCount int32
	if r.IsFlexible() {
		groupCount, o = compactArrayLength(payload[offset:])
		offset += o
	} else {
		groupCount = int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
	}
	if groupCount >= 0 {
		r.Groups = make([]*Group, groupCount)
		for i := int32(0); i < groupCount; i++ {
			r.Groups[i] = &Group{}
			if r.IsFlexible() {
				r.Groups[i].GroupID, o = compactString(payload[offset:])
				offset += o
				r.Groups[i].ProtocolType, o = compactString(payload[offset:])
				offset += o
				if version >= tags["GroupState"] {
					r.Groups[i].GroupState, o = compactString(payload[offset:])
					offset += o
				}
				if version >= tags["GroupType"] {
					r.Groups[i].GroupType, o = compactString(payload[offset:])
					offset += o
				}
			} else {
				r.Groups[i].GroupID, o = nonnullableString(payload[offset:])
				offset += o
				r.Groups[i].ProtocolType, o = nonnullableString(payload[offset:])
				offset += o
				if version >= tags["GroupState"] {
					r.Groups[i].GroupState, o = nonnullableString(payload[offset:])
					offset += o
				}
				if version >= tags["GroupType"] {
					r.Groups[i].GroupType, o = nonnullableString(payload[offset:])
					offset += o
				}
			}

			if r.IsFlexible() {
				r.Groups[i].TaggedFields, o = DecodeTaggedFields(payload[offset:])
				offset += o
			}
		}
	}
	if r.IsFlexible() {
		r.TaggedFields, o = DecodeTaggedFields(payload[offset:])
		offset += o
	}

	return r, nil
}

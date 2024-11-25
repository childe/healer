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

func (r *ListGroupsResponse) length() (n int) {
	n = 4 + r.ResponseHeader.length()
	n += 4 // ThrottleTimeMS
	n += 2 // ErrorCode
	n += 4 // GroupCount
	for _, group := range r.Groups {
		n += 2 + len(group.GroupID)
		n += 2 + len(group.ProtocolType)
		n += 2 + len(group.GroupState)
		n += 2 + len(group.GroupType)
		n += group.TaggedFields.length()
	}
	n += r.TaggedFields.length()
	return n
}

// just for test
func (r *ListGroupsResponse) Encode(version uint16) (payload []byte) {
	payload = make([]byte, r.length())
	offset := 4 // payload length
	tags := r.tags()

	defer func() {
		binary.BigEndian.PutUint32(payload, uint32(offset-4))
	}()

	offset += r.ResponseHeader.EncodeTo(payload[offset:])

	if version >= tags["ThrottleTimeMS"] {
		binary.BigEndian.PutUint32(payload[offset:], uint32(r.ThrottleTimeMS))
		offset += 4
	}

	binary.BigEndian.PutUint16(payload[offset:], r.ErrorCode)
	offset += 2

	var groupCount int
	if r.Groups == nil {
		groupCount = -1
	} else {
		groupCount = len(r.Groups)
	}
	if r.IsFlexible() {
		offset += copy(payload[offset:], encodeCompactArrayLength(groupCount))
	} else {
		binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Groups)))
		offset += 4
	}

	for _, group := range r.Groups {
		if r.IsFlexible() {
			offset += copy(payload[offset:], encodeCompactString(group.GroupID))
			offset += copy(payload[offset:], encodeCompactString(group.ProtocolType))
			if version >= tags["GroupState"] {
				offset += copy(payload[offset:], encodeCompactString(group.GroupState))
			}
			if version >= tags["GroupType"] {
				offset += copy(payload[offset:], encodeCompactString(group.GroupType))
			}
		} else {
			offset += copy(payload[offset:], encodeString(group.GroupID))
			offset += copy(payload[offset:], encodeString(group.ProtocolType))
			if version >= tags["GroupState"] {
				offset += copy(payload[offset:], encodeString(group.GroupState))
			}
			if version >= tags["GroupType"] {
				offset += copy(payload[offset:], encodeString(group.GroupType))
			}
		}

		if r.IsFlexible() {
			offset += group.TaggedFields.EncodeTo(payload[offset:])
		}
	}

	if r.IsFlexible() {
		offset += r.TaggedFields.EncodeTo(payload[offset:])
	}

	payload = payload[:offset]
	return payload
}

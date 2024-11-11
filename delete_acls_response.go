package healer

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type DeleteAclsMatchingAcl struct {
	ErrorCode      int16
	ErrorMessage   *string
	ResourceType   AclsResourceType
	ResourceName   string
	PatternType    AclsPatternType
	Principal      string
	Host           string
	Operation      AclsOperation
	PermissionType AclsPermissionType
	TaggedFields   TaggedFields
}

func DecodeDeleteAclsMatchingAcl(payload []byte, version uint16, isFlexible bool) (m DeleteAclsMatchingAcl, offset int) {
	var o int

	m.ErrorCode = int16(binary.BigEndian.Uint16(payload))
	offset += 2

	if isFlexible {
		m.ErrorMessage, o = compactNullableString(payload[offset:])
		offset += o
	} else {
		m.ErrorMessage, o = nullableString(payload[offset:])
		offset += o
	}

	m.ResourceType = AclsResourceType(payload[offset])
	offset++

	if isFlexible {
		m.ResourceName, o = compactString(payload[offset:])
		offset += o
	} else {
		m.ResourceName, o = nonnullableString(payload[offset:])
		offset += o
	}

	if version >= 1 {
		m.PatternType = AclsPatternType(payload[offset])
		offset++
	}

	if isFlexible {
		m.Principal, o = compactString(payload[offset:])
		offset += o
		m.Host, o = compactString(payload[offset:])
		offset += o
	} else {
		m.Principal, o = nonnullableString(payload[offset:])
		offset += o
		m.Host, o = nonnullableString(payload[offset:])
		offset += o
	}

	m.Operation = AclsOperation(payload[offset])
	offset++

	m.PermissionType = AclsPermissionType(payload[offset])
	offset++

	if isFlexible {
		m.TaggedFields, o = DecodeTaggedFields(payload[offset:])
		offset += o
	}

	return
}

// just used in test
func (m *DeleteAclsMatchingAcl) encode(version uint16, isFlexible bool) []byte {
	buf := &bytes.Buffer{}
	binary.Write(buf, binary.BigEndian, m.ErrorCode)
	if isFlexible {
		buf.Write(encodeCompactNullableString(m.ErrorMessage))
	} else {
		buf.Write(encodeNullableString(m.ErrorMessage))
	}

	buf.WriteByte(byte(m.ResourceType))
	if isFlexible {
		buf.Write(encodeCompactString(m.ResourceName))
	} else {
		buf.Write(encodeString(m.ResourceName))
	}
	if version >= 1 {
		buf.WriteByte(byte(m.PatternType))
	}
	if isFlexible {
		buf.Write(encodeCompactString(m.Principal))
		buf.Write(encodeCompactString(m.Host))
	} else {
		buf.Write(encodeString(m.Principal))
		buf.Write(encodeString(m.Host))
	}
	buf.WriteByte(byte(m.Operation))
	buf.WriteByte(byte(m.PermissionType))
	if isFlexible {
		buf.Write(m.TaggedFields.Encode())
	}
	return buf.Bytes()
}

type DeleteAclsFilterResult struct {
	ErrorCode    int16
	ErrorMessage *string
	MatchingAcls []DeleteAclsMatchingAcl
	TaggedFields TaggedFields
}

type DeleteAclsResponse struct {
	ResponseHeader
	ThrottleTimeMs int32
	FilterResults  []DeleteAclsFilterResult
	TaggedFields   TaggedFields
}

func (r *DeleteAclsResponse) Error() (err error) {
	for i := range r.FilterResults {
		if r.FilterResults[i].ErrorCode != 0 {
			return fmt.Errorf("%w: %s", KafkaError(r.FilterResults[i].ErrorCode), *r.FilterResults[i].ErrorMessage)
		}
		for j := range r.FilterResults[i].MatchingAcls {
			if r.FilterResults[i].MatchingAcls[j].ErrorCode != 0 {
				return fmt.Errorf("%w: %s", KafkaError(r.FilterResults[i].MatchingAcls[j].ErrorCode),
					*r.FilterResults[i].MatchingAcls[j].ErrorMessage)
			}
		}
	}
	return nil
}

func DecodeDeleteAclsResponse(payload []byte, version uint16) (*DeleteAclsResponse, error) {
	var (
		r          = &DeleteAclsResponse{}
		offset int = 0
		o      int
	)

	responseLength := int32(binary.BigEndian.Uint32(payload))
	if responseLength+4 != int32(len(payload)) {
		return r, fmt.Errorf("DeleteAclsResponse length did not match: %d!=%d", responseLength+4, len(payload))
	}
	offset += 4

	r.ResponseHeader, o = DecodeResponseHeader(payload[offset:], API_DeleteAcls, version)
	offset += o

	r.ThrottleTimeMs = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	if r.ResponseHeader.IsFlexible() {
		filterCount, o := compactArrayLength(payload[offset:])
		offset += o
		r.FilterResults = make([]DeleteAclsFilterResult, filterCount)
	} else {
		filterCount := int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
		r.FilterResults = make([]DeleteAclsFilterResult, filterCount)
	}

	for i := range r.FilterResults {
		r.FilterResults[i].ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
		offset += 2

		if r.ResponseHeader.IsFlexible() {
			r.FilterResults[i].ErrorMessage, o = compactNullableString(payload[offset:])
			offset += o
		} else {
			r.FilterResults[i].ErrorMessage, o = nullableString(payload[offset:])
			offset += o
		}

		if r.ResponseHeader.IsFlexible() {
			matchingAclCount, o := compactArrayLength(payload[offset:])
			offset += o
			r.FilterResults[i].MatchingAcls = make([]DeleteAclsMatchingAcl, matchingAclCount)
		} else {
			matchingAclCount := int32(binary.BigEndian.Uint32(payload[offset:]))
			offset += 4
			r.FilterResults[i].MatchingAcls = make([]DeleteAclsMatchingAcl, matchingAclCount)
		}

		for j := range r.FilterResults[i].MatchingAcls {
			r.FilterResults[i].MatchingAcls[j], o = DecodeDeleteAclsMatchingAcl(payload[offset:],
				r.ResponseHeader.apiVersion, r.ResponseHeader.IsFlexible())
			offset += o
		}

		r.FilterResults[i].TaggedFields, o = DecodeTaggedFields(payload[offset:])
		offset += o
	}

	r.TaggedFields, o = DecodeTaggedFields(payload[offset:])
	offset += o

	return r, nil
}

// just for test
func (r *DeleteAclsResponse) Encode() (payload []byte) {
	buf := &bytes.Buffer{}

	// length placeholder
	binary.Write(buf, binary.BigEndian, int32(0))
	defer func() {
		binary.BigEndian.PutUint32(payload, uint32(len(payload)-4))
	}()

	buf.Write(r.ResponseHeader.Encode())

	binary.Write(buf, binary.BigEndian, r.ThrottleTimeMs)

	if r.ResponseHeader.IsFlexible() {
		buf.Write(encodeCompactArrayLength(len(r.FilterResults)))
	} else {
		binary.Write(buf, binary.BigEndian, int32(len(r.FilterResults)))
	}

	for _, fr := range r.FilterResults {
		binary.Write(buf, binary.BigEndian, fr.ErrorCode)
		if r.ResponseHeader.IsFlexible() {
			buf.Write(encodeCompactNullableString(fr.ErrorMessage))
		} else {
			buf.Write(encodeNullableString(fr.ErrorMessage))
		}

		if r.ResponseHeader.IsFlexible() {
			buf.Write(encodeCompactArrayLength(len(fr.MatchingAcls)))
		} else {
			binary.Write(buf, binary.BigEndian, int32(len(fr.MatchingAcls)))
		}

		for _, m := range fr.MatchingAcls {
			buf.Write(m.encode(r.ResponseHeader.apiVersion, r.ResponseHeader.IsFlexible()))
		}

		buf.Write(fr.TaggedFields.Encode())
	}

	buf.Write(r.TaggedFields.Encode())
	return buf.Bytes()
}

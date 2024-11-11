package healer

import (
	"bytes"
	"encoding/binary"
)

type DescribeAclsRequest struct {
	RequestHeader
	DescribeAclsRequestBody
}
type DescribeAclsRequestBody struct {
	ResourceType   AclsResourceType
	ResourceName   *string
	PatternType    AclsPatternType
	Principal      *string
	Host           *string
	Operation      AclsOperation
	PermissionType AclsPermissionType
	TaggedFields   TaggedFields
}

func (r *DescribeAclsRequest) Encode(version uint16) (rst []byte) {
	buf := new(bytes.Buffer)

	// length
	binary.Write(buf, binary.BigEndian, uint32(0))
	defer func() {
		length := len(rst) - 4
		binary.BigEndian.PutUint32(rst, uint32(length))
	}()

	header := make([]byte, r.RequestHeader.length())
	l := r.RequestHeader.EncodeTo(header)
	buf.Write(header[:l])

	binary.Write(buf, binary.BigEndian, r.ResourceType)

	if version < 2 {
		writeNullableString(buf, r.ResourceName)
	} else {
		writeCompactNullableString(buf, r.ResourceName)
	}

	if version >= 1 {
		binary.Write(buf, binary.BigEndian, r.PatternType)
	}

	if version < 2 {
		writeNullableString(buf, r.Principal)
		writeNullableString(buf, r.Host)
	} else {
		writeCompactNullableString(buf, r.Principal)
		writeCompactNullableString(buf, r.Host)
	}

	binary.Write(buf, binary.BigEndian, r.Operation)

	binary.Write(buf, binary.BigEndian, r.PermissionType)

	buf.Write(r.DescribeAclsRequestBody.TaggedFields.Encode())

	return buf.Bytes()
}

// just for test
func DecodeDescribeAclsRequest(payload []byte, version uint16) (r DescribeAclsRequest, err error) {
	offset := 0

	// request payload length
	binary.BigEndian.Uint32(payload)
	offset += 4

	header, o := DecodeRequestHeader(payload[offset:])
	r.RequestHeader = header
	offset += o

	r.ResourceType = AclsResourceType(payload[offset])
	offset++

	if version < 2 {
		resourceName, o := nullableString(payload[offset:])
		offset += o
		r.ResourceName = resourceName
	} else {
		resourceName, o := compactNullableString(payload[offset:])
		offset += o
		r.ResourceName = resourceName
	}

	if version >= 1 {
		r.PatternType = AclsPatternType(payload[offset])
		offset++
	}

	if version < 2 {
		principal, o := nullableString(payload[offset:])
		offset += o
		r.Principal = principal

		host, o := nullableString(payload[offset:])
		offset += o
		r.Host = host
	} else {
		principal, o := compactNullableString(payload[offset:])
		offset += o
		r.Principal = principal

		host, o := compactNullableString(payload[offset:])
		offset += o
		r.Host = host
	}

	r.Operation = AclsOperation(payload[offset])
	offset++

	r.PermissionType = AclsPermissionType(payload[offset])
	offset++

	return r, nil
}

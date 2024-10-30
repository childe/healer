package healer

import (
	"bytes"
	"encoding/binary"
)

type DescribeAclsResponse struct {
	CorrelationID uint32

	ThrottleTimeMs int32
	ErrorCode      int16
	ErrorMessage   string
	Resources      []AclResource
}

type AclResource struct {
	ResourceType int8
	ResourceName string
	PatternType  int8
	Acls         []Acl
}

type Acl struct {
	Principal      string
	Host           string
	Operation      int8
	PermissionType int8
}

func (r DescribeAclsResponse) Error() error {
	if r.ErrorCode != 0 {
		return KafkaError(r.ErrorCode)
	}
	return nil
}

func NewDescribeAclsResponse(payload []byte, version uint16) (response DescribeAclsResponse, err error) {
	offset := 0

	// Skip message size
	offset += 4

	response.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	offset++ // TaggedFields

	// ThrottleTimeMs
	response.ThrottleTimeMs = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	// ErrorCode
	response.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	// ErrorMessage
	if str, n := compactNullableString(payload[offset:]); n > 0 {
		response.ErrorMessage = str
		offset += n
	} else {
		offset += 1
	}

	// Resources array
	resourceCount, o := compactArrayLength(payload[offset:])
	offset += o
	response.Resources = make([]AclResource, resourceCount)

	for i := uint64(0); i < resourceCount; i++ {
		// ResourceType
		response.Resources[i].ResourceType = int8(payload[offset])
		offset++

		// ResourceName
		if str, n := compactString(payload[offset:]); n > 0 {
			response.Resources[i].ResourceName = str
			offset += n
		}

		// PatternType
		if version >= 1 {
			response.Resources[i].PatternType = int8(payload[offset])
			offset++
		}

		// Acls array
		aclCount, o := compactArrayLength(payload[offset:])
		offset += o
		response.Resources[i].Acls = make([]Acl, aclCount)

		for j := uint64(0); j < aclCount; j++ {
			// Principal
			if str, n := compactString(payload[offset:]); n > 0 {
				response.Resources[i].Acls[j].Principal = str
				offset += n
			}

			// Host
			if str, n := compactString(payload[offset:]); n > 0 {
				response.Resources[i].Acls[j].Host = str
				offset += n
			}

			// Operation
			response.Resources[i].Acls[j].Operation = int8(payload[offset])
			offset++

			// PermissionType
			response.Resources[i].Acls[j].PermissionType = int8(payload[offset])
			offset++

			offset++ // TaggedFields
		}
		offset++ // TaggedFields
	}
	offset++ // TaggedFields

	return response, nil
}

func (r *DescribeAclsResponse) Encode(version uint16) (rst []byte, err error) {
	defer func() {
		length := len(rst) - 4
		binary.BigEndian.PutUint32(rst, uint32(length))
	}()

	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, uint32(0))

	binary.Write(buf, binary.BigEndian, r.CorrelationID)
	binary.Write(buf, binary.BigEndian, r.ThrottleTimeMs)
	binary.Write(buf, binary.BigEndian, r.ErrorCode)

	writeNullableString(buf, r.ErrorMessage)

	binary.Write(buf, binary.BigEndian, uint32(len(r.Resources)))

	for _, resource := range r.Resources {
		buf.WriteByte(byte(resource.ResourceType))

		writeNullableString(buf, resource.ResourceName)

		if version >= 1 {
			buf.WriteByte(byte(resource.PatternType))
		}

		binary.Write(buf, binary.BigEndian, uint32(len(resource.Acls)))

		for _, acl := range resource.Acls {
			writeNullableString(buf, acl.Principal)

			writeNullableString(buf, acl.Host)

			buf.WriteByte(byte(acl.Operation))
			buf.WriteByte(byte(acl.PermissionType))
		}
	}

	return buf.Bytes(), nil

}

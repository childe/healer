package healer

import (
	"bytes"
	"encoding/binary"
)

type DescribeAclsResponse struct {
	CorrelationID uint32

	ThrottleTimeMs int32
	ErrorCode      int16
	ErrorMessage   *string
	Resources      []AclResource
	TaggedFields   TaggedFields
}

type AclResource struct {
	ResourceType int8
	ResourceName string
	PatternType  int8
	Acls         []Acl
	TaggedFields TaggedFields
}

type Acl struct {
	Principal      string
	Host           string
	Operation      int8
	PermissionType int8
	TaggedFields   TaggedFields
}

func (r DescribeAclsResponse) Error() error {
	if r.ErrorCode != 0 {
		return KafkaError(r.ErrorCode)
	}
	return nil
}

func NewDescribeAclsResponse(payload []byte, version uint16) (response DescribeAclsResponse, err error) {
	var o int
	offset := 0

	// Skip message size
	offset += 4

	response.CorrelationID = binary.BigEndian.Uint32(payload[offset:])
	offset += 4

	if version >= 1 {
		_, o = DecodeTaggedFields(payload[offset:], version)
		offset += o
	}

	// ThrottleTimeMs
	response.ThrottleTimeMs = int32(binary.BigEndian.Uint32(payload[offset:]))
	offset += 4

	// ErrorCode
	response.ErrorCode = int16(binary.BigEndian.Uint16(payload[offset:]))
	offset += 2

	// ErrorMessage
	if version <= 1 {
		str, n := nullableString(payload[offset:])
		response.ErrorMessage = str
		offset += n
	} else {
		str, n := compactNullableString(payload[offset:])
		response.ErrorMessage = str
		offset += n
	}

	// Resources array
	var (
		resourceCount int32
	)
	if version <= 1 {
		resourceCount = int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
	} else {
		c, o := compactArrayLength(payload[offset:])
		resourceCount = c
		offset += o
	}
	if resourceCount == -1 {
		response.Resources = nil
	} else {
		response.Resources = make([]AclResource, resourceCount)
	}

	for i := 0; i < len(response.Resources); i++ {
		// ResourceType
		response.Resources[i].ResourceType = int8(payload[offset])
		offset++

		// ResourceName
		if version <= 1 {
			str, n := nonnullableString(payload[offset:])
			response.Resources[i].ResourceName = str
			offset += n
		} else {
			str, n := compactString(payload[offset:])
			response.Resources[i].ResourceName = str
			offset += n
		}

		// PatternType
		if version >= 1 {
			response.Resources[i].PatternType = int8(payload[offset])
			offset++
		}

		// Acls array
		var aclCount uint32
		if version <= 1 {
			aclCount = binary.BigEndian.Uint32(payload[offset:])
			offset += 4
		} else {
			c, o := compactArrayLength(payload[offset:])
			aclCount = uint32(c)
			offset += o
		}
		response.Resources[i].Acls = make([]Acl, aclCount)

		for j := uint32(0); j < aclCount; j++ {
			if version <= 1 {
				str, n := nonnullableString(payload[offset:])
				response.Resources[i].Acls[j].Principal = str
				offset += n

				str, n = nonnullableString(payload[offset:])
				response.Resources[i].Acls[j].Host = str
				offset += n
			} else {
				str, n := compactString(payload[offset:])
				response.Resources[i].Acls[j].Principal = str
				offset += n

				str, n = compactString(payload[offset:])
				response.Resources[i].Acls[j].Host = str
				offset += n
			}

			// Operation
			response.Resources[i].Acls[j].Operation = int8(payload[offset])
			offset++

			// PermissionType
			response.Resources[i].Acls[j].PermissionType = int8(payload[offset])
			offset++

			if version >= 2 {
				response.Resources[i].Acls[j].TaggedFields, o = DecodeTaggedFields(payload[offset:], version)
				offset += o
			}
		}
		if version >= 2 {
			response.Resources[i].TaggedFields, o = DecodeTaggedFields(payload[offset:], version)
			offset += o
		}
	}
	if version >= 2 {
		response.TaggedFields, o = DecodeTaggedFields(payload[offset:], version)
		offset += o
	}

	return response, nil
}

// just for test
func (r *DescribeAclsResponse) Encode(version uint16) (rst []byte, err error) {
	defer func() {
		length := len(rst) - 4
		binary.BigEndian.PutUint32(rst, uint32(length))
	}()

	buf := new(bytes.Buffer)
	// length
	binary.Write(buf, binary.BigEndian, uint32(0))

	binary.Write(buf, binary.BigEndian, r.CorrelationID)
	if version >= 1 {
		buf.Write(r.TaggedFields.Encode())
	}
	binary.Write(buf, binary.BigEndian, r.ThrottleTimeMs)
	binary.Write(buf, binary.BigEndian, r.ErrorCode)

	if version <= 1 {
		writeNullableString(buf, r.ErrorMessage)
	} else {
		writeCompactNullableString(buf, r.ErrorMessage)
	}

	if version <= 1 {
		binary.Write(buf, binary.BigEndian, uint32(len(r.Resources)))
	} else {
		buf.Write(encodeCompactArrayLength(len(r.Resources)))
	}

	for _, resource := range r.Resources {
		buf.WriteByte(byte(resource.ResourceType))

		if version <= 1 {
			writeString(buf, resource.ResourceName)
		} else {
			writeCompactString(buf, resource.ResourceName)
		}

		if version >= 1 {
			buf.WriteByte(byte(resource.PatternType))
		}

		if version <= 1 {
			binary.Write(buf, binary.BigEndian, uint32(len(resource.Acls)))
		} else {
			buf.Write(encodeCompactArrayLength(len(resource.Acls)))
		}

		for _, acl := range resource.Acls {
			if version <= 1 {
				writeString(buf, acl.Principal)
				writeString(buf, acl.Host)
			} else {
				writeCompactString(buf, acl.Principal)
				writeCompactString(buf, acl.Host)
			}

			buf.WriteByte(byte(acl.Operation))
			buf.WriteByte(byte(acl.PermissionType))

			if version >= 2 {
				buf.Write(acl.TaggedFields.Encode())
			}
		}
		if version >= 2 {
			buf.Write(resource.TaggedFields.Encode())
		}
	}

	if version >= 2 {
		buf.Write(r.TaggedFields.Encode())
	}
	return buf.Bytes(), nil

}

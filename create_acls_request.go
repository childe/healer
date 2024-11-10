package healer

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type CreateAclsRequest struct {
	RequestHeader
	Creations    []AclCreation
	TaggedFields TaggedFields
}

type AclCreation struct {
	ResourceType        AclsResourceType
	ResourceName        string
	ResourcePatternType AclsPatternType
	Principal           string
	Host                string
	Operation           AclsOperation
	PermissionType      AclsPermissionType
	TaggedFields        TaggedFields
}

func (a *AclCreation) length() (n int) {
	n += 1                       // resource type
	n += 2 + len(a.ResourceName) // resource name
	n += 1                       // resource pattern type
	n += 2 + len(a.Principal)    // principal
	n += 2 + len(a.Host)         // host
	n += 1                       // operation
	n += 1                       // permission type
	n += a.TaggedFields.length()
	return
}

func (a *AclCreation) encode(version uint16) []byte {
	buf := bytes.NewBuffer(make([]byte, 0, a.length()))
	binary.Write(buf, binary.BigEndian, a.ResourceType)
	writeCompactString(buf, a.ResourceName)
	binary.Write(buf, binary.BigEndian, a.ResourcePatternType)
	writeCompactString(buf, a.Principal)
	writeCompactString(buf, a.Host)
	binary.Write(buf, binary.BigEndian, a.Operation)
	binary.Write(buf, binary.BigEndian, a.PermissionType)
	buf.Write(a.TaggedFields.Encode())
	return buf.Bytes()
}

func (r *CreateAclsRequest) length() (n int) {
	n += r.RequestHeader.length()
	n += 4 // number of creations
	for i := range r.Creations {
		n += r.Creations[i].length()
	}
	n += r.TaggedFields.length()
	n += 4 // payload length
	return
}

func (r *CreateAclsRequest) Encode() (payload []byte, err error) {
	payload = make([]byte, r.length())
	offset := 0
	var headerVersion uint16 = r.RequestHeader.headerVersion()

	defer func() {
		binary.BigEndian.PutUint32(payload, uint32(offset-4))
		payload = payload[:offset]
	}()

	offset += 4 // skip the length field
	offset += r.RequestHeader.Encode(payload[offset:])

	// Encode the number of creations
	if headerVersion < 1 {
		binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Creations)))
		offset += 4
	} else {
		offset += copy(payload[offset:], encodeCompactArrayLength(len(r.Creations)))
	}

	// Encode each creation
	for _, creation := range r.Creations {
		payload[offset] = uint8(creation.ResourceType)
		offset++

		if headerVersion < 1 {
			offset += (copy(payload[offset:], encodeString(creation.ResourceName)))
		} else {
			offset += (copy(payload[offset:], encodeCompactString(creation.ResourceName)))
		}

		payload[offset] = uint8(creation.ResourcePatternType)
		offset++

		if headerVersion < 1 {
			offset += (copy(payload[offset:], encodeString(creation.Principal)))
			offset += (copy(payload[offset:], encodeString(creation.Host)))
		} else {
			offset += (copy(payload[offset:], encodeCompactString(creation.Principal)))
			offset += (copy(payload[offset:], encodeCompactString(creation.Host)))
		}
		payload[offset] = uint8(creation.Operation)
		offset++
		payload[offset] = uint8(creation.PermissionType)
		offset++
		offset += copy(payload[offset:], creation.TaggedFields.Encode())
	}

	// Write the tag buffer
	offset += copy(payload[offset:], r.TaggedFields.Encode())

	return payload, nil
}

// just for test
func DecodeCreateAclsRequest(payload []byte, version uint16) (r CreateAclsRequest, err error) {
	offset := 0
	o := 0

	requestLength := binary.BigEndian.Uint32(payload)
	offset += 4
	if requestLength != uint32(len(payload)-4) {
		return r, fmt.Errorf("offsets response length did not match: %d!=%d", requestLength+4, len(payload))
	}

	// Decode RequestHeader
	r.RequestHeader, o = DecodeRequestHeader(payload[offset:], version)
	offset += o
	headerVersion := r.RequestHeader.headerVersion()

	// Decode number of creations
	var numCreations int32
	if r.RequestHeader.headerVersion() < 1 {
		numCreations = int32(binary.BigEndian.Uint32(payload[offset:]))
		offset += 4
	} else {
		numCreations, o = compactArrayLength(payload[offset:])
		offset += o
	}

	// Decode each creation
	r.Creations = make([]AclCreation, numCreations)
	for i := int32(0); i < numCreations; i++ {
		creation := &AclCreation{}

		creation.ResourceType = AclsResourceType(payload[offset])
		offset++

		if headerVersion < 1 {
			creation.ResourceName, o = nonnullableString(payload[offset:])
			offset += o
		} else {
			creation.ResourceName, o = compactString(payload[offset:])
			offset += o
		}

		creation.ResourcePatternType = AclsPatternType(payload[offset])
		offset++

		if headerVersion < 1 {
			creation.Principal, o = nonnullableString(payload[offset:])
			offset += o
			creation.Host, o = nonnullableString(payload[offset:])
			offset += o
		} else {
			creation.Principal, o = compactString(payload[offset:])
			offset += o
			creation.Host, o = compactString(payload[offset:])
			offset += o
		}

		creation.Operation = AclsOperation(payload[offset])
		offset++

		creation.PermissionType = AclsPermissionType(payload[offset])
		offset++

		creation.TaggedFields, offset = DecodeTaggedFields(payload[offset:], version)

		r.Creations[i] = *creation
	}

	// Decode TaggedFields
	r.TaggedFields, o = DecodeTaggedFields(payload[offset:], version)
	offset += o

	return r, nil
}

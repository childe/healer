package healer

import (
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

func (a *AclCreation) encodeTo(payload []byte, version uint16, headerVersion uint16) (offset int) {
	payload[offset] = uint8(a.ResourceType)
	offset++

	if headerVersion < 2 {
		offset += (copy(payload[offset:], encodeString(a.ResourceName)))
	} else {
		offset += (copy(payload[offset:], encodeCompactString(a.ResourceName)))
	}

	if version >= 1 {
		payload[offset] = uint8(a.ResourcePatternType)
		offset++
	}

	if headerVersion < 2 {
		offset += (copy(payload[offset:], encodeString(a.Principal)))
		offset += (copy(payload[offset:], encodeString(a.Host)))
	} else {
		offset += (copy(payload[offset:], encodeCompactString(a.Principal)))
		offset += (copy(payload[offset:], encodeCompactString(a.Host)))
	}
	payload[offset] = uint8(a.Operation)
	offset++
	payload[offset] = uint8(a.PermissionType)
	offset++
	offset += copy(payload[offset:], a.TaggedFields.Encode())

	return
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

func (r *CreateAclsRequest) Encode(version uint16) (payload []byte) {
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
	if headerVersion < 2 {
		binary.BigEndian.PutUint32(payload[offset:], uint32(len(r.Creations)))
		offset += 4
	} else {
		offset += copy(payload[offset:], encodeCompactArrayLength(len(r.Creations)))
	}

	// Encode each creation
	for _, creation := range r.Creations {
		offset += creation.encodeTo(payload[offset:], r.APIVersion, headerVersion)
	}

	// Write the tag buffer
	offset += copy(payload[offset:], r.TaggedFields.Encode())

	return payload
}

// just for test
func DecodeCreateAclsRequest(payload []byte) (r CreateAclsRequest, err error) {
	offset := 0
	o := 0

	requestLength := binary.BigEndian.Uint32(payload)
	offset += 4
	if requestLength != uint32(len(payload)-4) {
		return r, fmt.Errorf("offsets response length did not match: %d!=%d", requestLength+4, len(payload))
	}

	// Decode RequestHeader
	r.RequestHeader, o = DecodeRequestHeader(payload[offset:])
	offset += o
	headerVersion := r.RequestHeader.headerVersion()

	// Decode number of creations
	var numCreations int32
	if headerVersion < 2 {
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

		if headerVersion < 2 {
			creation.ResourceName, o = nonnullableString(payload[offset:])
			offset += o
		} else {
			creation.ResourceName, o = compactString(payload[offset:])
			offset += o
		}

		if r.RequestHeader.APIVersion >= 1 {
			creation.ResourcePatternType = AclsPatternType(payload[offset])
			offset++
		}

		if headerVersion < 2 {
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

		creation.TaggedFields, o = DecodeTaggedFields(payload[offset:])
		offset += o

		r.Creations[i] = *creation
	}

	// Decode TaggedFields
	r.TaggedFields, o = DecodeTaggedFields(payload[offset:])
	offset += o

	return r, nil
}

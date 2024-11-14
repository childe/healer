package healer

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
)

// version0
type ListGroupsRequest struct {
	*RequestHeader
	IncludeAuthorizedOperations bool `healer:"minversion:3"`
}

var tagCacheListGroupsRequest atomic.Value

func NewListGroupsRequest(clientID string) *ListGroupsRequest {
	requestHeader := &RequestHeader{
		APIKey:     API_ListGroups,
		APIVersion: 0,
		ClientID:   &clientID,
	}
	return &ListGroupsRequest{
		RequestHeader:               requestHeader,
		IncludeAuthorizedOperations: true,
	}
}

func (r *ListGroupsRequest) length() (n int) {
	n = r.RequestHeader.length()
	n += 1 // IncludeAuthorizedOperations
	n += 4 // payload length
	return
}

func (r *ListGroupsRequest) tags() (fieldsVersions map[string]uint16) {
	if v := tagCacheListGroupsRequest.Load(); v != nil {
		return v.(map[string]uint16)
	}

	fieldsVersions = make(map[string]uint16)
	defer tagCacheListGroupsRequest.Store(fieldsVersions)

	for i := 0; i < reflect.ValueOf(*r).NumField(); i++ {
		field := reflect.TypeOf(*r).Field(i)
		healerTag := field.Tag.Get("healer")
		if healerTag != "" {
			parts := strings.Split(healerTag, ":")
			if len(parts) > 1 && parts[1] != "" && parts[0] == "minversion" {
				if ver, err := strconv.Atoi(parts[1]); err == nil {
					fieldsVersions[field.Name] = uint16(ver)
				}
			}
		}
	}
	return fieldsVersions
}

func (r *ListGroupsRequest) Encode(version uint16) (payload []byte) {
	tags := r.tags()

	payload = make([]byte, r.length())
	offset := 4 // payload length

	defer func() {
		binary.BigEndian.PutUint32(payload, uint32(offset-4))
	}()

	offset += r.RequestHeader.EncodeTo(payload[offset:])

	if r.APIVersion >= tags[`IncludeAuthorizedOperations`] {
		if r.IncludeAuthorizedOperations {
			payload[offset] = 1
		} else {
			payload[offset] = 0
		}
		offset++
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

	if r.APIVersion >= tags[`IncludeAuthorizedOperations`] {
		r.IncludeAuthorizedOperations = payload[offset] != 0
		offset++
	}

	return r, nil
}

package healer

type Error struct {
	Errorcode int
	ErrorMsg  string
	ErrorDesc string
}

func (healerError *Error) Error() string {
	return healerError.ErrorDesc
}

var AllError []*Error = make([]*Error, 32)

func init() {
	AllError[0] = &Error{
		Errorcode: -1,
		ErrorMsg:  "Unknown",
		ErrorDesc: "The server experienced an unexpected error when processing the request",
	}
	AllError[1] = &Error{
		Errorcode: 1,
		ErrorMsg:  "OFFSET_OUT_OF_RANGE",
		ErrorDesc: "The requested offset is not within the range of offsets maintained by the server.",
	}
	AllError[2] = &Error{
		Errorcode: 2,
		ErrorMsg:  "CORRUPT_MESSAGE",
		ErrorDesc: "This message has failed its CRC checksum, exceeds the valid size, or is otherwise corrupt.",
	}
	AllError[3] = &Error{
		Errorcode: 3,
		ErrorMsg:  "UNKNOWN_TOPIC_OR_PARTITION",
		ErrorDesc: "This server does not host this topic-partition.",
	}
	AllError[4] = &Error{
		Errorcode: 4,
		ErrorMsg:  "INVALID_FETCH_SIZE",
		ErrorDesc: "The requested fetch size is invalid.",
	}
	AllError[5] = &Error{
		Errorcode: 5,
		ErrorMsg:  "INVALID_FETCH_SIZE",
		ErrorDesc: "The requested fetch size is invalid.",
	}
	AllError[6] = &Error{
		Errorcode: 6,
		ErrorMsg:  "NOT_LEADER_FOR_PARTITION",
		ErrorDesc: "This server is not the leader for that topic-partition.",
	}

	AllError[400] = &Error{
		Errorcode: 400,
		ErrorMsg:  "MAX_BYTES TOO SMALL",
		ErrorDesc: "MaxBytes parameter is to small for server to send back one whole message.",
	}
}

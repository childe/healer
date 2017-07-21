package healer

type Error struct {
	Errorcode int
	ErrorMsg  string
	ErrorDesc string
}

func (healerError *Error) Format() string {
	return healerError.ErrorDesc
}

var AllError []*Error = make([]*Error, 32)

func init() {
	AllError[0] = &Error{
		Errorcode: -1,
		ErrorMsg:  "Unknown",
		ErrorDesc: "An unexpected server error",
	}
	AllError[1] = &Error{
		Errorcode: 1,
		ErrorMsg:  "OffsetOutOfRange",
		ErrorDesc: "The requested offset is outside the range of offsets maintained by the server for the given topic/partition.",
	}
	AllError[2] = &Error{
		Errorcode: 2,
		ErrorMsg:  "InvalidMessage",
		ErrorDesc: "This indicates that a message contents does not match its CRC",
	}
	AllError[3] = &Error{
		Errorcode: 3,
		ErrorMsg:  "UnknownTopicOrPartition",
		ErrorDesc: "This indicates that a message contents does not match its CRC",
	}
	AllError[4] = &Error{
		Errorcode: 4,
		ErrorMsg:  "InvalidMessageSize",
		ErrorDesc: "The message has a negative size",
	}
}

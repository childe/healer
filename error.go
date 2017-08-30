package healer

type HealerError int32

func (healerError *HealerError) Error() string {
	switch *healerError {
	case 1:
		return "fetch response is truncated because of max-bytes parameter in fetch request."
	case 2:
		return "MaxBytes parameter is too small for server to send back one whole message."
	}
	return ""
}

var (
	fetchResponseTruncated HealerError = 1
	maxBytesTooSmall       HealerError = 2
)

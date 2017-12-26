package healer

type HealerError int32

func (healerError *HealerError) Error() string {
	switch *healerError {
	case 1:
		return "fetch response is truncated because of max-bytes parameter in fetch request."
	case 2:
		return "MaxBytes parameter is too small for server to send back one whole message."
	case 3:
		return "could not read more data from fetch response, but not get topicname yet"
	case 4:
		return "NO partitionResponse?"
	case 5:
		return "fetch response is truncated"
	}
	return ""
}

var (
	fetchResponseTruncatedInMessageSet     HealerError = 1
	maxBytesTooSmall                       HealerError = 2
	notEnoughDataInFetchResponse           HealerError = 3
	noPartitionResponse                    HealerError = 4
	fetchResponseTruncatedBeforeMessageSet HealerError = 5
)

package healer

// https://developers.google.com/protocol-buffers/docs/encoding

// ReadVarint decodes from buf, returns value and buf length that contains the value
func ReadVarint(buf []byte) (value int64, length int) {
	l := len(buf)
	for length < l {
		b := buf[length]
		if b&0x80 == 0 {
			value = (int64(b) << (7 * length)) | value
			length++
			value = (value ^ 1) >> 1
			return
		}
		value = (int64(b&0x7f) << (7 * length)) | value
		length++
	}
	value = (value ^ 1) >> 1
	return
}

// EncodeVarint encodes int64 to []byte
func EncodeVarint(value int64) []byte {
	value = (value << 1) ^ 1
	ans := make([]byte, 1)
	for value > 0 {
		ans = append(ans, byte((value&0x7f)|0x80))
		value >>= 7
	}
	ans[len(ans)-1] &= 0x7f
	return ans
}

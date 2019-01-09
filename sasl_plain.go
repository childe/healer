package healer

type PlainSasl struct {
	user     string
	password string
}

func NewPlainSasl(user, password string) *PlainSasl {
	return &PlainSasl{
		user,
		password,
	}
}

func (p *PlainSasl) Encode() []byte {
	r := make([]byte, 2+len(p.user)+len(p.password))
	copy(r, []byte("\x00"+p.user+"\x00"+p.password))
	return r
}

package healer

import (
	"testing"
)

func TestGenSaslHandShakeRequest(t *testing.T) {
	var (
		clientID  string = "healer"
		mechanism string = "PLAIN"
	)

	saslHandShakeRequest := NewSaslHandShakeRequest(clientID, mechanism)

	payload := saslHandShakeRequest.Encode(0)
	if len(payload) != 27 {
		t.Error("SaslHandShakeRequest payload length should be 27")
	}
}

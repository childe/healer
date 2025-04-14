package healer

import "io"

type NullString string

type String string

func (s *NullString) WriteTo(w io.Writer, isFlexible bool) (n int64, err error) {
	return 0, nil
}

func (s *String) WriteTo(w io.Writer, isFlexible bool) (n int64, err error) {
	return 0, nil
}

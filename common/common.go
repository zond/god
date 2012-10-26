package common

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"testing"
	"time"
)

const (
	Redundancy = 3
)

func AssertWithin(t *testing.T, f func() (string, bool), d time.Duration) {
	deadline := time.Now().Add(d)
	var ok bool
	var msg string
	for time.Now().Before(deadline) {
		if msg, ok = f(); ok {
			return
		}
		time.Sleep(time.Second)
	}
	t.Errorf("wanted %v to be true within %v, but it never happened: %v", f, d, msg)
}

func HexEncode(b []byte) (result string) {
	encoded := hex.EncodeToString(b)
	buffer := new(bytes.Buffer)
	for i := len(encoded); i < len(b)*2; i++ {
		fmt.Fprint(buffer, "00")
	}
	fmt.Fprint(buffer, encoded)
	return string(buffer.Bytes())
}

func Between(needle, fromInc, toExc []byte) (result bool) {
	switch bytes.Compare(fromInc, toExc) {
	case 0:
		result = true
	case -1:
		result = bytes.Compare(fromInc, needle) < 1 && bytes.Compare(needle, toExc) < 0
	case 1:
		result = bytes.Compare(fromInc, needle) < 1 || bytes.Compare(needle, toExc) < 0
	default:
		panic("Shouldn't happen")
	}
	return
}

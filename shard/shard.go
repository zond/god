package shard

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func hexEncode(b []byte) (result string) {
	encoded := hex.EncodeToString(b)
	buffer := new(bytes.Buffer)
	for i := len(encoded); i < len(b)*2; i++ {
		fmt.Fprint(buffer, "00")
	}
	fmt.Fprint(buffer, encoded)
	return string(buffer.Bytes())
}

func between(needle, start, end []byte) (result bool) {
	switch bytes.Compare(start, end) {
	case 0:
		result = true
	case -1:
		result = bytes.Compare(start, needle) < 1 && bytes.Compare(needle, end) < 0
	case 1:
		result = bytes.Compare(start, needle) < 1 || bytes.Compare(needle, end) < 0
	default:
		panic("Shouldn't happen")
	}
	return
}

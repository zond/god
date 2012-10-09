package shard

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func findAddress() (addr string, err error) {
	var udpAddr *net.UDPAddr
	if udpAddr, err = net.ResolveUDPAddr("udp", "www.internic.net:80"); err != nil {
		return
	}
	var udpConn *net.UDPConn
	if udpConn, err = net.DialUDP("udp", nil, udpAddr); err != nil {
		return
	}
	addr = udpConn.LocalAddr().String()
	return
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

func between(needle, fromInc, toExc []byte) (result bool) {
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

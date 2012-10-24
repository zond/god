package common

import (
	"bytes"
	"fmt"
	"net/rpc"
)

type Remote struct {
	Pos  []byte
	Addr string
}

func (self *Remote) EqualP(other *Remote) bool {
	if self == nil {
		if other == nil {
			return true
		}
		return false
	}
	if other == nil {
		return false
	}
	return (*self).Equal(*other)
}
func (self Remote) Equal(other Remote) bool {
	return self.Addr == other.Addr && bytes.Compare(self.Pos, other.Pos) == 0
}
func (self Remote) less(other Remote) bool {
	val := bytes.Compare(self.Pos, other.Pos)
	if val == 0 {
		val = bytes.Compare([]byte(self.Addr), []byte(other.Addr))
	}
	return val < 0
}
func (self Remote) String() string {
	return fmt.Sprintf("[%v@%v]", HexEncode(self.Pos), self.Addr)
}
func (self Remote) Call(service string, args, reply interface{}) error {
	return Switch.Call(self.Addr, service, args, reply)
}
func (self Remote) Go(service string, args, reply interface{}) *rpc.Call {
	return Switch.Go(self.Addr, service, args, reply)
}

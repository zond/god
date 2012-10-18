package client

import (
	"../common"
)

type Conn struct {
	nodes *common.Ring
}

func NewConn(addr string) (result *Conn, err error) {
	result = &Conn{&common.Ring{}}
	err = common.Switch.Call(addr, "Node.Ring", 0, result.nodes)
	return
}
func MustConn(addr string) (result *Conn) {
	var err error
	if result, err = NewConn(addr); err != nil {
		panic(err)
	}
	return
}
func (self *Conn) Describe() string {
	return self.nodes.Describe()
}

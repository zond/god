package client

import (
	"../common"
)

type Conn struct {
	ring *common.Ring
}

func NewConn(addr string) (result *Conn, err error) {
	result = &Conn{&common.Ring{}}
	err = common.Switch.Call(addr, "Node.Ring", 0, result.ring)
	return
}
func MustConn(addr string) (result *Conn) {
	var err error
	if result, err = NewConn(addr); err != nil {
		panic(err)
	}
	return
}
func (self *Conn) Put(key, value []byte) (old []byte, exists bool, err error) {
	item := common.Item{
		Key:   key,
		Value: value,
	}
	predecessor, match, _ := self.ring.Remotes(key)
	if match != nil {
		predecessor = match
	}
	if err = predecessor.Call("DHash.Put", item, &item); err != nil {
		return
	}
	old = item.Value
	exists = item.Exists
	return
}
func (self *Conn) Describe() string {
	return self.ring.Describe()
}

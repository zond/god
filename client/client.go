package client

import (
	"../common"
	"fmt"
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
func (self *Conn) Put(key, value []byte) (err error) {
	item := common.Item{
		Key:   key,
		Value: value,
	}
	predecessor, match, _ := self.ring.Remotes(key)
	if match != nil {
		predecessor = match
	}
	var x int
	err = predecessor.Call("DHash.Put", item, &x)
	return
}
func (self *Conn) DescribeTree(key []byte) (result string, err error) {
	_, match, _ := self.ring.Remotes(key)
	if match == nil {
		err = fmt.Errorf("No node with position %v found", common.HexEncode(key))
		return
	}
	err = match.Call("DHash.DescribeTree", 0, &result)
	return
}
func (self *Conn) Describe() string {
	return self.ring.Describe()
}

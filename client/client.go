package client

import (
	"../common"
	"bytes"
	"fmt"
	"net/rpc"
	"sync/atomic"
	"time"
)

const (
	created = iota
	started
	stopped
)

type Conn struct {
	ring  *common.Ring
	state int32
}

func NewConnRing(ring *common.Ring) *Conn {
	return &Conn{ring: ring}
}
func NewConn(addr string) (result *Conn, err error) {
	result = &Conn{ring: common.NewRing()}
	var newNodes common.Remotes
	err = common.Switch.Call(addr, "Node.Nodes", 0, &newNodes)
	result.ring.SetNodes(newNodes)
	return
}
func MustConn(addr string) (result *Conn) {
	var err error
	if result, err = NewConn(addr); err != nil {
		panic(err)
	}
	return
}
func (self *Conn) hasState(s int32) bool {
	return atomic.LoadInt32(&self.state) == s
}
func (self *Conn) changeState(old, neu int32) bool {
	return atomic.CompareAndSwapInt32(&self.state, old, neu)
}
func (self *Conn) update() {
	myRingHash := self.ring.Hash()
	var otherRingHash []byte
	node := self.ring.Random()
	if err := node.Call("DHash.RingHash", 0, &otherRingHash); err != nil {
		self.ring.Remove(node)
		self.Reconnect()
		return
	}
	if bytes.Compare(myRingHash, otherRingHash) != 0 {
		var newNodes common.Remotes
		if err := node.Call("Node.Nodes", 0, &newNodes); err != nil {
			self.ring.Remove(node)
			self.Reconnect()
			return
		}
		self.ring.SetNodes(newNodes)
	}
}
func (self *Conn) updateRegularly() {
	for self.hasState(started) {
		self.update()
		time.Sleep(common.PingInterval)
	}
}
func (self *Conn) Start() {
	if self.changeState(created, started) {
		go self.updateRegularly()
	}
}
func (self *Conn) Reconnect() {
	node := self.ring.Random()
	var err error
	for {
		var newNodes common.Remotes
		if err = node.Call("Node.Ring", 0, &newNodes); err == nil {
			self.ring.SetNodes(newNodes)
			return
		}
		self.ring.Remove(node)
		if self.ring.Size() == 0 {
			panic(fmt.Errorf("%v doesn't know of any live nodes!", self))
		}
		node = self.ring.Random()
	}
}
func (self *Conn) Put(key, value []byte) {
	data := common.Item{
		Key:   key,
		Value: value,
	}
	_, _, successor := self.ring.Remotes(key)
	var x int
	if err := successor.Call("DHash.Put", data, &x); err != nil {
		self.Reconnect()
		self.Put(key, value)
	}
}
func (self *Conn) Get(key []byte) (value []byte, existed bool) {
	data := common.Item{
		Key: key,
	}
	currentRedundancy := self.ring.Redundancy()
	futures := make([]*rpc.Call, currentRedundancy)
	results := make([]*common.Item, currentRedundancy)
	nextKey := key
	var nextSuccessor *common.Remote
	for i := 0; i < currentRedundancy; i++ {
		_, _, nextSuccessor = self.ring.Remotes(nextKey)
		result := &common.Item{}
		results[i] = result
		futures[i] = nextSuccessor.Go("DHash.Get", data, result)
		nextKey = nextSuccessor.Pos
	}
	var result *common.Item
	for index, future := range futures {
		<-future.Done
		if future.Error != nil {
			self.Reconnect()
			return self.Get(key)
		}
		if result == nil || result.Timestamp < results[index].Timestamp {
			result = results[index]
		}
	}
	value, existed = result.Value, result.Exists
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

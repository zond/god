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
	err = common.Switch.Call(addr, "Discord.Nodes", 0, &newNodes)
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
func (self *Conn) removeNode(node common.Remote) {
	self.ring.Remove(node)
	self.Reconnect()
}
func (self *Conn) Nodes() common.Remotes {
	return self.ring.Nodes()
}
func (self *Conn) update() {
	myRingHash := self.ring.Hash()
	var otherRingHash []byte
	node := self.ring.Random()
	if err := node.Call("DHash.RingHash", 0, &otherRingHash); err != nil {
		self.removeNode(node)
		return
	}
	if bytes.Compare(myRingHash, otherRingHash) != 0 {
		var newNodes common.Remotes
		if err := node.Call("Discord.Nodes", 0, &newNodes); err != nil {
			self.removeNode(node)
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
		if err = node.Call("Discord.Nodes", 0, &newNodes); err == nil {
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

func (self *Conn) subDel(key, subKey []byte, sync bool) {
	data := common.Item{
		Key:    key,
		SubKey: subKey,
		Sync:   sync,
	}
	_, _, successor := self.ring.Remotes(key)
	var x int
	if err := successor.Call("DHash.SubDel", data, &x); err != nil {
		self.removeNode(*successor)
		self.subDel(key, subKey, sync)
	}
}
func (self *Conn) subPut(key, subKey, value []byte, sync bool) {
	data := common.Item{
		Key:    key,
		SubKey: subKey,
		Value:  value,
		Sync:   sync,
	}
	_, _, successor := self.ring.Remotes(key)
	var x int
	if err := successor.Call("DHash.SubPut", data, &x); err != nil {
		self.removeNode(*successor)
		self.subPut(key, subKey, value, sync)
	}
}
func (self *Conn) del(key []byte, sync bool) {
	data := common.Item{
		Key:  key,
		Sync: sync,
	}
	_, _, successor := self.ring.Remotes(key)
	var x int
	if err := successor.Call("DHash.Del", data, &x); err != nil {
		self.removeNode(*successor)
		self.del(key, sync)
	}
}
func (self *Conn) put(key, value []byte, sync bool) {
	data := common.Item{
		Key:   key,
		Value: value,
		Sync:  sync,
	}
	_, _, successor := self.ring.Remotes(key)
	var x int
	if err := successor.Call("DHash.Put", data, &x); err != nil {
		self.removeNode(*successor)
		self.put(key, value, sync)
	}
}
func (self *Conn) mergeRecent(operation string, r common.Range, up bool) (result []common.Item) {
	currentRedundancy := self.ring.Redundancy()
	futures := make([]*rpc.Call, currentRedundancy)
	results := make([]*[]common.Item, currentRedundancy)
	nodes := make(common.Remotes, currentRedundancy)
	nextKey := r.Key
	var nextSuccessor *common.Remote
	for i := 0; i < currentRedundancy; i++ {
		_, _, nextSuccessor = self.ring.Remotes(nextKey)
		var thisResult []common.Item
		nodes[i] = *nextSuccessor
		results[i] = &thisResult
		futures[i] = nextSuccessor.Go(operation, r, &thisResult)
		nextKey = nextSuccessor.Pos
	}
	for index, future := range futures {
		<-future.Done
		if future.Error != nil {
			self.removeNode(nodes[index])
			return self.mergeRecent(operation, r, up)
		}
	}
	result = common.MergeItems(results, up)
	return
}
func (self *Conn) findRecent(operation string, data common.Item) (result *common.Item) {
	currentRedundancy := self.ring.Redundancy()
	futures := make([]*rpc.Call, currentRedundancy)
	results := make([]*common.Item, currentRedundancy)
	nodes := make(common.Remotes, currentRedundancy)
	nextKey := data.Key
	var nextSuccessor *common.Remote
	for i := 0; i < currentRedundancy; i++ {
		_, _, nextSuccessor = self.ring.Remotes(nextKey)
		thisResult := &common.Item{}
		nodes[i] = *nextSuccessor
		results[i] = thisResult
		futures[i] = nextSuccessor.Go(operation, data, thisResult)
		nextKey = nextSuccessor.Pos
	}
	for index, future := range futures {
		<-future.Done
		if future.Error != nil {
			self.removeNode(nodes[index])
			return self.findRecent(operation, data)
		}
		if result == nil || result.Timestamp < results[index].Timestamp {
			result = results[index]
		}
	}
	return
}

func (self *Conn) SSubPut(key, subKey, value []byte) {
	self.subPut(key, subKey, value, true)
}
func (self *Conn) SubPut(key, subKey, value []byte) {
	self.subPut(key, subKey, value, false)
}
func (self *Conn) SPut(key, value []byte) {
	self.put(key, value, true)
}
func (self *Conn) Put(key, value []byte) {
	self.put(key, value, false)
}
func (self *Conn) SubDel(key, subKey []byte) {
	self.subDel(key, subKey, false)
}
func (self *Conn) SSubDel(key, subKey []byte) {
	self.subDel(key, subKey, true)
}
func (self *Conn) SDel(key []byte) {
	self.del(key, true)
}
func (self *Conn) Del(key []byte) {
	self.del(key, false)
}
func (self *Conn) ReverseIndexOf(key, subKey []byte) (index int, existed bool) {
	data := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	_, _, successor := self.ring.Remotes(key)
	var result common.Index
	if err := successor.Call("DHash.ReverseIndexOf", data, &result); err != nil {
		self.removeNode(*successor)
		return self.ReverseIndexOf(key, subKey)
	}
	index, existed = result.N, result.Existed
	return
}
func (self *Conn) IndexOf(key, subKey []byte) (index int, existed bool) {
	data := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	_, _, successor := self.ring.Remotes(key)
	var result common.Index
	if err := successor.Call("DHash.IndexOf", data, &result); err != nil {
		self.removeNode(*successor)
		return self.IndexOf(key, subKey)
	}
	index, existed = result.N, result.Existed
	return
}
func (self *Conn) Next(key []byte) (nextKey, nextValue []byte, existed bool) {
	data := common.Item{
		Key: key,
	}
	result := &common.Item{}
	_, _, successor := self.ring.Remotes(key)
	firstAddr := successor.Addr
	for {
		if err := successor.Call("DHash.Next", data, result); err != nil {
			self.removeNode(*successor)
			return self.Next(key)
		}
		if result.Exists {
			break
		}
		_, _, successor = self.ring.Remotes(successor.Pos)
		if successor.Addr == firstAddr {
			break
		}
	}
	nextKey, nextValue, existed = result.Key, result.Value, result.Exists
	return
}
func (self *Conn) Prev(key []byte) (prevKey, prevValue []byte, existed bool) {
	data := common.Item{
		Key: key,
	}
	result := &common.Item{}
	_, _, successor := self.ring.Remotes(key)
	firstAddr := successor.Addr
	for {
		if err := successor.Call("DHash.Prev", data, result); err != nil {
			self.removeNode(*successor)
			return self.Prev(key)
		}
		if result.Exists {
			break
		}
		successor, _, _ = self.ring.Remotes(successor.Pos)
		if successor.Addr == firstAddr {
			break
		}
	}
	prevKey, prevValue, existed = result.Key, result.Value, result.Exists
	return
}
func (self *Conn) Count(key, min, max []byte, mininc, maxinc bool) (result int) {
	r := common.Range{
		Key:    key,
		Min:    min,
		Max:    max,
		MinInc: mininc,
		MaxInc: maxinc,
	}
	_, _, successor := self.ring.Remotes(key)
	if err := successor.Call("DHash.Count", r, &result); err != nil {
		self.removeNode(*successor)
		return self.Count(key, min, max, mininc, maxinc)
	}
	return
}
func (self *Conn) NextIndex(key []byte, index int) (foundKey, foundValue []byte, foundIndex int, existed bool) {
	data := common.Item{
		Key:   key,
		Index: index,
	}
	result := &common.Item{}
	_, _, successor := self.ring.Remotes(key)
	if err := successor.Call("DHash.NextIndex", data, result); err != nil {
		self.removeNode(*successor)
		return self.NextIndex(key, index)
	}
	foundKey, foundValue, foundIndex, existed = result.Key, result.Value, result.Index, result.Exists
	return
}
func (self *Conn) PrevIndex(key []byte, index int) (foundKey, foundValue []byte, foundIndex int, existed bool) {
	data := common.Item{
		Key:   key,
		Index: index,
	}
	result := &common.Item{}
	_, _, successor := self.ring.Remotes(key)
	if err := successor.Call("DHash.PrevIndex", data, result); err != nil {
		self.removeNode(*successor)
		return self.NextIndex(key, index)
	}
	foundKey, foundValue, foundIndex, existed = result.Key, result.Value, result.Index, result.Exists
	return
}
func (self *Conn) ReverseSliceIndex(key []byte, min, max *int) (result []common.Item) {
	r := common.Range{
		Key:      key,
		MinIndex: *min,
		MaxIndex: *max,
		MinInc:   min != nil,
		MaxInc:   max != nil,
	}
	result = self.mergeRecent("DHash.ReverseSliceIndex", r, false)
	return
}
func (self *Conn) SliceIndex(key []byte, min, max *int) (result []common.Item) {
	r := common.Range{
		Key:      key,
		MinIndex: *min,
		MaxIndex: *max,
		MinInc:   min != nil,
		MaxInc:   max != nil,
	}
	result = self.mergeRecent("DHash.SliceIndex", r, true)
	return
}
func (self *Conn) ReverseSlice(key, min, max []byte, mininc, maxinc bool) (result []common.Item) {
	r := common.Range{
		Key:    key,
		Min:    min,
		Max:    max,
		MinInc: mininc,
		MaxInc: maxinc,
	}
	result = self.mergeRecent("DHash.ReverseSlice", r, false)
	return
}
func (self *Conn) Slice(key, min, max []byte, mininc, maxinc bool) (result []common.Item) {
	r := common.Range{
		Key:    key,
		Min:    min,
		Max:    max,
		MinInc: mininc,
		MaxInc: maxinc,
	}
	result = self.mergeRecent("DHash.Slice", r, true)
	return
}
func (self *Conn) SubPrev(key, subKey []byte) (prevKey, prevValue []byte, existed bool) {
	data := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	result := self.findRecent("DHash.SubPrev", data)
	prevKey, prevValue, existed = result.Key, result.Value, result.Exists
	return
}
func (self *Conn) SubNext(key, subKey []byte) (nextKey, nextValue []byte, existed bool) {
	data := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	result := self.findRecent("DHash.SubNext", data)
	nextKey, nextValue, existed = result.Key, result.Value, result.Exists
	return
}
func (self *Conn) Last(key []byte) (lastKey, lastValue []byte, existed bool) {
	data := common.Item{
		Key: key,
	}
	result := self.findRecent("DHash.Last", data)
	lastKey, lastValue, existed = result.Key, result.Value, result.Exists
	return
}
func (self *Conn) First(key []byte) (firstKey, firstValue []byte, existed bool) {
	data := common.Item{
		Key: key,
	}
	result := self.findRecent("DHash.First", data)
	firstKey, firstValue, existed = result.Key, result.Value, result.Exists
	return
}
func (self *Conn) SubGet(key, subKey []byte) (value []byte, existed bool) {
	data := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	result := self.findRecent("DHash.SubGet", data)
	if result.Value != nil {
		value, existed = result.Value, result.Exists
	} else {
		value, existed = nil, false
	}
	return
}
func (self *Conn) Get(key []byte) (value []byte, existed bool) {
	data := common.Item{
		Key: key,
	}
	result := self.findRecent("DHash.Get", data)
	if result.Value != nil {
		value, existed = result.Value, result.Exists
	} else {
		value, existed = nil, false
	}
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
func (self *Conn) DescribeNode(key []byte) (result common.DHashDescription, err error) {
	_, match, _ := self.ring.Remotes(key)
	if match == nil {
		err = fmt.Errorf("No node with position %v found", common.HexEncode(key))
		return
	}
	err = match.Call("DHash.Describe", 0, &result)
	return
}
func (self *Conn) SubSize(key []byte) (result int) {
	_, _, successor := self.ring.Remotes(key)
	if err := successor.Call("DHash.SubSize", key, &result); err != nil {
		self.removeNode(*successor)
		return self.SubSize(key)
	}
	return
}
func (self *Conn) Size() (result int) {
	var tmp int
	for _, node := range self.ring.Nodes() {
		if err := node.Call("DHash.Size", 0, &tmp); err != nil {
			self.removeNode(node)
			return self.Size()
		}
		result += tmp
	}
	return
}
func (self *Conn) Describe() string {
	return self.ring.Describe()
}

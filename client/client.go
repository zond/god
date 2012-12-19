package client

import (
	"../common"
	"../setop"
	"bytes"
	"fmt"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

const (
	created = iota
	started
	stopped
)

func findKeys(op *setop.SetOp) (result map[string]bool) {
	result = make(map[string]bool)
	for _, source := range op.Sources {
		if source.Key != nil {
			result[string(source.Key)] = true
		} else {
			for key, _ := range findKeys(source.SetOp) {
				result[key] = true
			}
		}
	}
	return
}

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

func (self *Conn) subClear(key []byte, sync bool) {
	data := common.Item{
		Key:  key,
		Sync: sync,
	}
	_, _, successor := self.ring.Remotes(key)
	var x int
	if err := successor.Call("DHash.SubClear", data, &x); err != nil {
		self.removeNode(*successor)
		self.subClear(key, sync)
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
func (self *Conn) subPutVia(succ *common.Remote, key, subKey, value []byte, sync bool) {
	data := common.Item{
		Key:    key,
		SubKey: subKey,
		Value:  value,
		Sync:   sync,
	}
	var x int
	if err := succ.Call("DHash.SubPut", data, &x); err != nil {
		self.removeNode(*succ)
		_, _, newSuccessor := self.ring.Remotes(key)
		*succ = *newSuccessor
		self.subPutVia(succ, key, subKey, value, sync)
	}
}
func (self *Conn) subPut(key, subKey, value []byte, sync bool) {
	_, _, successor := self.ring.Remotes(key)
	self.subPutVia(successor, key, subKey, value, sync)
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
func (self *Conn) putVia(succ *common.Remote, key, value []byte, sync bool) {
	data := common.Item{
		Key:   key,
		Value: value,
		Sync:  sync,
	}
	var x int
	if err := succ.Call("DHash.Put", data, &x); err != nil {
		self.removeNode(*succ)
		_, _, newSuccessor := self.ring.Remotes(key)
		*succ = *newSuccessor
		self.putVia(succ, key, value, sync)
	}
}
func (self *Conn) put(key, value []byte, sync bool) {
	_, _, successor := self.ring.Remotes(key)
	self.putVia(successor, key, value, sync)
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
func (self *Conn) consume(c chan [2][]byte, wait *sync.WaitGroup, successor *common.Remote) {
	for pair := range c {
		self.putVia(successor, pair[0], pair[1], false)
	}
	wait.Done()
}
func (self *Conn) dump(c chan [2][]byte, wait *sync.WaitGroup) {
	var succ *common.Remote
	dumps := make(map[string]chan [2][]byte)
	for pair := range c {
		_, _, succ = self.ring.Remotes(pair[0])
		if dump, ok := dumps[succ.Addr]; ok {
			dump <- pair
		} else {
			newDump := make(chan [2][]byte, 16)
			wait.Add(1)
			go self.consume(newDump, wait, succ)
			newDump <- pair
			dumps[succ.Addr] = newDump
		}
	}
	for _, dump := range dumps {
		close(dump)
	}
	wait.Done()
}
func (self *Conn) subDump(key []byte, c chan [2][]byte, wait *sync.WaitGroup) {
	_, _, succ := self.ring.Remotes(key)
	for pair := range c {
		self.subPutVia(succ, key, pair[0], pair[1], false)
	}
	wait.Done()
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
func (self *Conn) Dump() (c chan [2][]byte, wait *sync.WaitGroup) {
	wait = new(sync.WaitGroup)
	c = make(chan [2][]byte, 16)
	wait.Add(1)
	go self.dump(c, wait)
	return
}
func (self *Conn) SubDump(key []byte) (c chan [2][]byte, wait *sync.WaitGroup) {
	wait = new(sync.WaitGroup)
	c = make(chan [2][]byte)
	wait.Add(1)
	go self.subDump(key, c, wait)
	return
}
func (self *Conn) SubClear(key []byte) {
	self.subClear(key, false)
}
func (self *Conn) SSubClear(key []byte) {
	self.subClear(key, true)
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
func (self *Conn) MirrorReverseIndexOf(key, subKey []byte) (index int, existed bool) {
	data := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	_, _, successor := self.ring.Remotes(key)
	var result common.Index
	if err := successor.Call("DHash.MirrorReverseIndexOf", data, &result); err != nil {
		self.removeNode(*successor)
		return self.MirrorReverseIndexOf(key, subKey)
	}
	index, existed = result.N, result.Existed
	return
}
func (self *Conn) MirrorIndexOf(key, subKey []byte) (index int, existed bool) {
	data := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	_, _, successor := self.ring.Remotes(key)
	var result common.Index
	if err := successor.Call("DHash.MirrorIndexOf", data, &result); err != nil {
		self.removeNode(*successor)
		return self.MirrorIndexOf(key, subKey)
	}
	index, existed = result.N, result.Existed
	return
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
func (self *Conn) MirrorCount(key, min, max []byte, mininc, maxinc bool) (result int) {
	r := common.Range{
		Key:    key,
		Min:    min,
		Max:    max,
		MinInc: mininc,
		MaxInc: maxinc,
	}
	_, _, successor := self.ring.Remotes(key)
	if err := successor.Call("DHash.MirrorCount", r, &result); err != nil {
		self.removeNode(*successor)
		return self.MirrorCount(key, min, max, mininc, maxinc)
	}
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
func (self *Conn) MirrorNextIndex(key []byte, index int) (foundKey, foundValue []byte, foundIndex int, existed bool) {
	data := common.Item{
		Key:   key,
		Index: index,
	}
	result := &common.Item{}
	_, _, successor := self.ring.Remotes(key)
	if err := successor.Call("DHash.MirrorNextIndex", data, result); err != nil {
		self.removeNode(*successor)
		return self.MirrorNextIndex(key, index)
	}
	foundKey, foundValue, foundIndex, existed = result.Key, result.Value, result.Index, result.Exists
	return
}
func (self *Conn) MirrorPrevIndex(key []byte, index int) (foundKey, foundValue []byte, foundIndex int, existed bool) {
	data := common.Item{
		Key:   key,
		Index: index,
	}
	result := &common.Item{}
	_, _, successor := self.ring.Remotes(key)
	if err := successor.Call("DHash.MirrorPrevIndex", data, result); err != nil {
		self.removeNode(*successor)
		return self.MirrorNextIndex(key, index)
	}
	foundKey, foundValue, foundIndex, existed = result.Key, result.Value, result.Index, result.Exists
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
func (self *Conn) MirrorReverseSliceIndex(key []byte, min, max *int) (result []common.Item) {
	r := common.Range{
		Key:      key,
		MinIndex: *min,
		MaxIndex: *max,
		MinInc:   min != nil,
		MaxInc:   max != nil,
	}
	result = self.mergeRecent("DHash.MirrorReverseSliceIndex", r, false)
	return
}
func (self *Conn) MirrorSliceIndex(key []byte, min, max *int) (result []common.Item) {
	r := common.Range{
		Key:      key,
		MinIndex: *min,
		MaxIndex: *max,
		MinInc:   min != nil,
		MaxInc:   max != nil,
	}
	result = self.mergeRecent("DHash.MirrorSliceIndex", r, true)
	return
}
func (self *Conn) MirrorReverseSlice(key, min, max []byte, mininc, maxinc bool) (result []common.Item) {
	r := common.Range{
		Key:    key,
		Min:    min,
		Max:    max,
		MinInc: mininc,
		MaxInc: maxinc,
	}
	result = self.mergeRecent("DHash.MirrorReverseSlice", r, false)
	return
}
func (self *Conn) MirrorSlice(key, min, max []byte, mininc, maxinc bool) (result []common.Item) {
	r := common.Range{
		Key:    key,
		Min:    min,
		Max:    max,
		MinInc: mininc,
		MaxInc: maxinc,
	}
	result = self.mergeRecent("DHash.MirrorSlice", r, true)
	return
}
func (self *Conn) MirrorSliceLen(key, min []byte, mininc bool, maxRes int) (result []common.Item) {
	r := common.Range{
		Key:    key,
		Min:    min,
		MinInc: mininc,
		Len:    maxRes,
	}
	result = self.mergeRecent("DHash.MirrorSliceLen", r, true)
	return
}
func (self *Conn) MirrorReverseSliceLen(key, max []byte, maxinc bool, maxRes int) (result []common.Item) {
	r := common.Range{
		Key:    key,
		Max:    max,
		MaxInc: maxinc,
		Len:    maxRes,
	}
	result = self.mergeRecent("DHash.MirrorReverseSliceLen", r, true)
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
func (self *Conn) SliceLen(key, min []byte, mininc bool, maxRes int) (result []common.Item) {
	r := common.Range{
		Key:    key,
		Min:    min,
		MinInc: mininc,
		Len:    maxRes,
	}
	result = self.mergeRecent("DHash.SliceLen", r, true)
	return
}
func (self *Conn) ReverseSliceLen(key, max []byte, maxinc bool, maxRes int) (result []common.Item) {
	r := common.Range{
		Key:    key,
		Max:    max,
		MaxInc: maxinc,
		Len:    maxRes,
	}
	result = self.mergeRecent("DHash.ReverseSliceLen", r, true)
	return
}
func (self *Conn) SubMirrorPrev(key, subKey []byte) (prevKey, prevValue []byte, existed bool) {
	data := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	result := self.findRecent("DHash.SubMirrorPrev", data)
	prevKey, prevValue, existed = result.Key, result.Value, result.Exists
	return
}
func (self *Conn) SubMirrorNext(key, subKey []byte) (nextKey, nextValue []byte, existed bool) {
	data := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	result := self.findRecent("DHash.SubMirrorNext", data)
	nextKey, nextValue, existed = result.Key, result.Value, result.Exists
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
func (self *Conn) MirrorLast(key []byte) (lastKey, lastValue []byte, existed bool) {
	data := common.Item{
		Key: key,
	}
	result := self.findRecent("DHash.MirrorLast", data)
	lastKey, lastValue, existed = result.Key, result.Value, result.Exists
	return
}
func (self *Conn) MirrorFirst(key []byte) (firstKey, firstValue []byte, existed bool) {
	data := common.Item{
		Key: key,
	}
	result := self.findRecent("DHash.MirrorFirst", data)
	firstKey, firstValue, existed = result.Key, result.Value, result.Exists
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
func (self *Conn) DescribeAllTrees() string {
	buf := new(bytes.Buffer)
	for _, rem := range self.ring.Nodes() {
		if res, err := self.DescribeTree(rem.Pos); err == nil {
			fmt.Println(res)
			fmt.Fprintln(buf, res)
		}
	}
	return string(buf.Bytes())
}
func (self *Conn) DescribeAllNodes() (result []common.DHashDescription) {
	for _, rem := range self.ring.Nodes() {
		if res, err := self.DescribeNode(rem.Pos); err == nil {
			result = append(result, res)
		}
	}
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

func (self *Conn) SetExpression(expr setop.SetExpression) (result []setop.SetOpResult) {
	var biggestKey []byte
	biggestSize := 0
	var thisSize int
	for key, _ := range findKeys(expr.Op) {
		thisSize = self.SubSize([]byte(key))
		if biggestKey == nil {
			biggestKey = []byte(key)
			biggestSize = thisSize
		} else if thisSize > biggestSize {
			biggestKey = []byte(key)
			biggestSize = thisSize
		}
	}
	_, _, successor := self.ring.Remotes(biggestKey)
	var results []setop.SetOpResult
	err := successor.Call("DHash.SetExpression", expr, &results)
	for err != nil {
		self.removeNode(*successor)
		_, _, successor = self.ring.Remotes(biggestKey)
		err = successor.Call("DHash.SetExpression", expr, &results)
	}
	return results
}

func (self *Conn) Configuration() (conf map[string]string) {
	var result common.Conf
	_, _, successor := self.ring.Remotes(nil)
	if err := successor.Call("DHash.Configuration", 0, &result); err != nil {
		self.removeNode(*successor)
		return self.Configuration()
	}
	return result.Data
}
func (self *Conn) SubConfiguration(key []byte) (conf map[string]string) {
	var result common.Conf
	_, _, successor := self.ring.Remotes(nil)
	if err := successor.Call("DHash.SubConfiguration", key, &result); err != nil {
		self.removeNode(*successor)
		return self.Configuration()
	}
	return result.Data
}
func (self *Conn) AddConfiguration(key, value string) {
	conf := common.ConfItem{
		Key:   key,
		Value: value,
	}
	_, _, successor := self.ring.Remotes(nil)
	var x int
	if err := successor.Call("DHash.AddConfiguration", conf, &x); err != nil {
		self.removeNode(*successor)
		self.AddConfiguration(key, value)
	}
}
func (self *Conn) SubAddConfiguration(treeKey []byte, key, value string) {
	conf := common.ConfItem{
		TreeKey: treeKey,
		Key:     key,
		Value:   value,
	}
	_, _, successor := self.ring.Remotes(nil)
	var x int
	if err := successor.Call("DHash.SubAddConfiguration", conf, &x); err != nil {
		self.removeNode(*successor)
		self.AddConfiguration(key, value)
	}
}

package dhash

import (
	"../client"
	"../common"
	"../discord"
	"../murmur"
	"../radix"
	"../timenet"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	syncInterval  = time.Second * 1
	cleanInterval = time.Second * 1
)

const (
	created = iota
	started
	stopped
)

type DHash struct {
	state int32
	node  *discord.Node
	timer *timenet.Timer
	tree  *radix.Tree
}

func NewDHash(addr string) (result *DHash) {
	result = &DHash{
		node:  discord.NewNode(addr),
		state: created,
		tree:  radix.NewTree(),
	}
	result.timer = timenet.NewTimer((*dhashPeerProducer)(result))
	result.node.Export("Timer", (*timerServer)(result.timer))
	result.node.Export("DHash", (*dhashServer)(result))
	result.node.Export("HashTree", (*hashTreeServer)(result.tree))
	return
}
func (self *DHash) hasState(s int32) bool {
	return atomic.LoadInt32(&self.state) == s
}
func (self *DHash) changeState(old, neu int32) bool {
	return atomic.CompareAndSwapInt32(&self.state, old, neu)
}
func (self *DHash) AddChangeListener(f common.RingChangeListener) {
	self.node.AddChangeListener(f)
}
func (self *DHash) Stop() {
	if self.changeState(started, stopped) {
		self.node.Stop()
		self.timer.Stop()
	}
}
func (self *DHash) Start() (err error) {
	if !self.changeState(created, started) {
		return fmt.Errorf("%v can only be started when in state 'created'", self)
	}
	if err = self.node.Start(); err != nil {
		return
	}
	self.timer.Start()
	go self.syncPeriodically()
	go self.cleanPeriodically()
	return
}
func (self *DHash) sync() {
	fetched := 0
	distributed := 0
	nextSuccessor := self.node.GetSuccessor(self.node.GetPosition())
	for i := 0; i < self.node.Redundancy()-1; i++ {
		distributed += radix.NewSync(self.tree, (remoteHashTree)(nextSuccessor)).From(self.node.GetPredecessor().Pos).To(self.node.GetPosition()).Run().PutCount()
		fetched += radix.NewSync((remoteHashTree)(nextSuccessor), self.tree).From(self.node.GetPredecessor().Pos).To(self.node.GetPosition()).Run().PutCount()
		nextSuccessor = self.node.GetSuccessor(nextSuccessor.Pos)
	}
	if fetched != 0 || distributed != 0 {
		fmt.Println(self, "fetched", fetched, "keys and distributed", distributed, "keys")
	}
}
func (self *DHash) syncPeriodically() {
	for self.hasState(started) {
		self.sync()
		time.Sleep(syncInterval)
	}
}
func (self *DHash) cleanPeriodically() {
	for self.hasState(started) {
		self.clean()
		time.Sleep(cleanInterval)
	}
}
func (self *DHash) circularNext(key []byte) (nextKey []byte, existed bool) {
	if nextKey, _, _, existed = self.tree.Next(key); existed {
		return
	}
	nextKey = make([]byte, murmur.Size)
	if _, _, existed = self.tree.Get(nextKey); existed {
		return
	}
	nextKey, _, _, existed = self.tree.Next(nextKey)
	return
}
func (self *DHash) owners(key []byte) (owners common.Remotes, isOwner bool) {
	owners = append(owners, self.node.GetSuccessor(key))
	if owners[0].Addr == self.node.GetAddr() {
		isOwner = true
	}
	for i := 1; i < self.node.Redundancy(); i++ {
		owners = append(owners, self.node.GetSuccessor(owners[i-1].Pos))
		if owners[i].Addr == self.node.GetAddr() {
			isOwner = true
		}
	}
	return
}
func (self *DHash) clean() {
	deleted := 0
	put := 0
	if nextKey, existed := self.circularNext(self.node.GetPosition()); existed {
		if owners, isOwner := self.owners(nextKey); !isOwner {
			var sync *radix.Sync
			for index, owner := range owners {
				sync = radix.NewSync(self.tree, (remoteHashTree)(owner)).From(nextKey).To(owners[0].Pos)
				if index == len(owners)-2 {
					sync.Destroy()
				}
				sync.Run()
				deleted += sync.DelCount()
				put += sync.PutCount()
			}
		}
		if deleted != 0 || put != 0 {
			fmt.Println(self, "relocated", put, "keys while cleaning out", deleted, "keys")
		}
	}
}
func (self *DHash) MustStart() *DHash {
	if err := self.Start(); err != nil {
		panic(err)
	}
	return self
}
func (self *DHash) MustJoin(addr string) {
	self.timer.Conform(remotePeer(common.Remote{Addr: addr}))
	self.node.MustJoin(addr)
}
func (self *DHash) Time() time.Time {
	return time.Unix(0, self.timer.ContinuousTime())
}
func (self *DHash) Describe() string {
	return self.node.Describe()
}
func (self *DHash) DescribeTree() string {
	return self.tree.Describe()
}
func (self *DHash) client() *client.Conn {
	return client.NewConnRing(common.NewRingNodes(self.node.Nodes()))
}
func (self *DHash) SubFind(data common.Item, result *common.Item) error {
	*result = data
	result.Value, result.Exists = self.client().Get(data.Key)
	return nil
}
func (self *DHash) Find(data common.Item, result *common.Item) error {
	*result = data
	result.Value, result.Exists = self.client().Get(data.Key)
	return nil
}
func (self *DHash) Prev(data common.Item, result *common.Item) error {
	*result = data
	if key, value, timestamp, exists := self.tree.Prev(data.Key); exists {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			result.Key, result.Value, result.Timestamp, result.Exists = key, []byte(byteHasher), timestamp, exists
		}
	}
	return nil
}
func (self *DHash) RingHash(x int, ringHash *[]byte) error {
	*ringHash = self.node.RingHash()
	return nil
}
func (self *DHash) Next(data common.Item, result *common.Item) error {
	*result = data
	if key, value, timestamp, exists := self.tree.Next(data.Key); exists {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			result.Key, result.Value, result.Timestamp, result.Exists = key, []byte(byteHasher), timestamp, exists
		}
	}
	return nil
}
func (self *DHash) Last(data common.Item, result *common.Item) error {
	if key, value, timestamp, exists := self.tree.SubLast(data.Key); exists {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			result.Key, result.Value, result.Timestamp, result.Exists = key, []byte(byteHasher), timestamp, exists
		}
	}
	return nil
}
func (self *DHash) First(data common.Item, result *common.Item) error {
	if key, value, timestamp, exists := self.tree.SubFirst(data.Key); exists {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			result.Key, result.Value, result.Timestamp, result.Exists = key, []byte(byteHasher), timestamp, exists
		}
	}
	return nil
}
func (self *DHash) SubPrev(data common.Item, result *common.Item) error {
	if key, value, timestamp, exists := self.tree.SubPrev(data.Key, data.SubKey); exists {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			result.Key, result.Value, result.Timestamp, result.Exists = key, []byte(byteHasher), timestamp, exists
		}
	}
	return nil
}
func (self *DHash) SubNext(data common.Item, result *common.Item) error {
	if key, value, timestamp, exists := self.tree.SubNext(data.Key, data.SubKey); exists {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			result.Key, result.Value, result.Timestamp, result.Exists = key, []byte(byteHasher), timestamp, exists
		}
	}
	return nil
}
func (self *DHash) Get(data common.Item, result *common.Item) error {
	*result = data
	if value, timestamp, exists := self.tree.Get(data.Key); exists {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			result.Exists, result.Value, result.Timestamp = true, []byte(byteHasher), timestamp
		}
	}
	return nil
}
func (self *DHash) SliceIndex(r common.Range, items *[]common.Item) error {
	min := &r.MinIndex
	max := &r.MaxIndex
	if !r.MinInc {
		min = nil
	}
	if !r.MaxInc {
		max = nil
	}
	self.tree.SubEachBetweenIndex(r.Key, min, max, func(key []byte, value radix.Hasher, version int64) bool {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			*items = append(*items, common.Item{
				Key:       key,
				Value:     []byte(byteHasher),
				Timestamp: version,
			})
		}
		return true
	})
	return nil
}
func (self *DHash) ReverseSliceIndex(r common.Range, items *[]common.Item) error {
	min := &r.MinIndex
	max := &r.MaxIndex
	if !r.MinInc {
		min = nil
	}
	if !r.MaxInc {
		max = nil
	}
	self.tree.SubReverseEachBetweenIndex(r.Key, min, max, func(key []byte, value radix.Hasher, version int64) bool {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			*items = append(*items, common.Item{
				Key:       key,
				Value:     []byte(byteHasher),
				Timestamp: version,
			})
		}
		return true
	})
	return nil
}
func (self *DHash) ReverseSlice(r common.Range, items *[]common.Item) error {
	self.tree.SubReverseEachBetween(r.Key, r.Min, r.Max, r.MinInc, r.MaxInc, func(key []byte, value radix.Hasher, version int64) bool {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			*items = append(*items, common.Item{
				Key:       key,
				Value:     []byte(byteHasher),
				Timestamp: version,
			})
		}
		return true
	})
	return nil
}
func (self *DHash) Slice(r common.Range, items *[]common.Item) error {
	self.tree.SubEachBetween(r.Key, r.Min, r.Max, r.MinInc, r.MaxInc, func(key []byte, value radix.Hasher, version int64) bool {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			*items = append(*items, common.Item{
				Key:       key,
				Value:     []byte(byteHasher),
				Timestamp: version,
			})
		}
		return true
	})
	return nil
}
func (self *DHash) ReverseIndexOf(data common.Item, result *common.Index) error {
	result.N, result.Existed = self.tree.SubReverseIndexOf(data.Key, data.SubKey)
	return nil
}
func (self *DHash) IndexOf(data common.Item, result *common.Index) error {
	result.N, result.Existed = self.tree.SubIndexOf(data.Key, data.SubKey)
	return nil
}
func (self *DHash) SubGet(data common.Item, result *common.Item) error {
	*result = data
	if value, timestamp, exists := self.tree.SubGet(data.Key, data.SubKey); exists {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			result.Exists, result.Value, result.Timestamp = true, []byte(byteHasher), timestamp
		}
	}
	return nil
}
func (self *DHash) SubPut(data common.Item) error {
	successor := self.node.GetSuccessor(data.Key)
	if successor.Addr != self.node.GetAddr() {
		var x int
		return successor.Call("DHash.SubPut", data, &x)
	}
	data.TTL, data.Timestamp = self.node.Redundancy(), self.timer.ContinuousTime()
	return self.subPut(data)
}
func (self *DHash) Put(data common.Item) error {
	successor := self.node.GetSuccessor(data.Key)
	if successor.Addr != self.node.GetAddr() {
		var x int
		return successor.Call("DHash.Put", data, &x)
	}
	data.TTL, data.Timestamp = self.node.Redundancy(), self.timer.ContinuousTime()
	return self.put(data)
}
func (self *DHash) forwardOperation(data common.Item, operation string) {
	data.TTL--
	successor := self.node.GetSuccessor(self.node.GetPosition())
	var x int
	err := successor.Call(operation, data, &x)
	for err != nil {
		self.node.RemoveNode(successor)
		successor = self.node.GetSuccessor(self.node.GetPosition())
		err = successor.Call(operation, data, &x)
	}
}
func (self *DHash) subPut(data common.Item) error {
	if data.TTL > 1 {
		go self.forwardOperation(data, "DHash.SlaveSubPut")
	}
	self.tree.SubPut(data.Key, data.SubKey, radix.ByteHasher(data.Value), data.Timestamp)
	return nil
}
func (self *DHash) put(data common.Item) error {
	if data.TTL > 1 {
		go self.forwardOperation(data, "DHash.SlavePut")
	}
	self.tree.Put(data.Key, radix.ByteHasher(data.Value), data.Timestamp)
	return nil
}

package dhash

import (
	"../discord"
	"../radix"
	"../timenet"
	"sync"
	"time"
)

const (
	redundancy = 3
)

type remotePeer discord.Remote

func (self remotePeer) ActualTime() (result int64) {
	if err := (discord.Remote)(self).Call("Timer.ActualTime", 0, &result); err != nil {
		result = time.Now().UnixNano()
	}
	return
}

type dhashPeerProducer DHash

func (self *dhashPeerProducer) Peers() (result map[string]timenet.Peer) {
	result = make(map[string]timenet.Peer)
	for _, node := range (*DHash)(self).node.GetNodes() {
		result[node.Addr] = (remotePeer)(node)
	}
	return
}

type timerServer timenet.Timer

func (self *timerServer) ActualTime(x int, result *int64) error {
	*result = (*timenet.Timer)(self).ActualTime()
	return nil
}

type Item struct {
	Key       []byte
	Value     radix.Hasher
	Exists    bool
	Timestamp int64
	TTL       int
}

type dhashServer DHash

func (self *dhashServer) SlavePut(data Item, res *Item) error {
	return (*DHash)(self).put(data, res)
}
func (self *dhashServer) Put(data Item, res *Item) error {
	return (*DHash)(self).Put(data, res)
}

type DHash struct {
	lock  *sync.RWMutex
	node  *discord.Node
	timer *timenet.Timer
	tree  *radix.Tree
}

func NewDHash(addr string) (result *DHash) {
	result = &DHash{
		lock: &sync.RWMutex{},
		node: discord.NewNode(addr),
		tree: radix.NewTree(),
	}
	result.timer = timenet.NewTimer((*dhashPeerProducer)(result))
	result.node.Export("Timer", (*timerServer)(result.timer))
	result.node.Export("DHash", (*dhashServer)(result))
	return
}
func (self *DHash) MustStart() *DHash {
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.node.MustStart()
	self.timer.Start()
	return self
}
func (self *DHash) MustJoin(addr string) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.timer.Conform(remotePeer{Addr: addr})
	self.node.MustJoin(addr)
}
func (self *DHash) Time() time.Time {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return time.Unix(0, self.timer.ContinuousTime())
}
func (self *DHash) Put(data Item, res *Item) error {
	self.lock.RLock()
	if nodeCount := self.node.CountNodes(); nodeCount < redundancy {
		data.TTL = nodeCount
	} else {
		data.TTL = redundancy
	}
	data.Timestamp = self.timer.ContinuousTime()
	self.lock.RUnlock()
	return self.put(data, res)
}
func (self *DHash) forwardPut(data Item) {
	self.lock.RLock()
	successor := self.node.GetSuccessor(self.node.GetPosition())
	self.lock.RUnlock()
	err := successor.Call("DHash.SlavePut", data, &data)
	for err != nil {
		self.lock.RLock()
		self.node.RemoveNode(successor)
		successor = self.node.GetSuccessor(self.node.GetPosition())
		self.lock.RUnlock()
		err = successor.Call("DHash.SlavePut", data, &data)
	}
}
func (self *DHash) put(data Item, res *Item) error {
	if data.TTL > 0 {
		go self.forwardPut(data)
	}
	res.Value, res.Exists = self.tree.Put(data.Key, data.Value, data.Timestamp)
	return nil
}

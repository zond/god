package dhash

import (
	"../common"
	"../discord"
	"../radix"
	"../timenet"
	"sync"
	"time"
)

const (
	redundancy = 3
)

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
func (self *DHash) Describe() string {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.node.Describe()
}
func (self *DHash) AddTopologyListener(listener discord.TopologyListener) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.node.AddTopologyListener(listener)
}
func (self *DHash) Put(data common.Item, res *common.Item) error {
	self.lock.RLock()
	successor := self.node.GetSuccessor(data.Key)
	if successor.Addr != self.node.GetAddr() {
		return successor.Call("DHash.Put", data, res)
	}
	if nodeCount := self.node.CountNodes(); nodeCount < redundancy {
		data.TTL = nodeCount
	} else {
		data.TTL = redundancy
	}
	data.Timestamp = self.timer.ContinuousTime()
	self.lock.RUnlock()
	return self.put(data, res)
}
func (self *DHash) forwardPut(data common.Item) {
	data.TTL--
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
func (self *DHash) put(data common.Item, res *common.Item) error {
	if data.TTL > 1 {
		go self.forwardPut(data)
	}
	if old, exists := self.tree.Put(data.Key, radix.ByteHasher(data.Value), data.Timestamp); exists {
		res.Value, res.Exists = old.(radix.ByteHasher)
	}
	return nil
}

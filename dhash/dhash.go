package dhash

import (
	"../common"
	"../discord"
	"../radix"
	"../timenet"
	"fmt"
	"sync/atomic"
	"time"
)

const (
	syncInterval  = time.Second * 10
	cleanInterval = time.Second * 10
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
	nextSuccessor := self.node.GetSuccessor(self.node.GetPosition())
	for i := 0; i < common.Redundancy; i++ {
		radix.NewSync(self.tree, (remoteHashTree)(nextSuccessor)).From(self.node.GetPredecessor().Pos).To(self.node.GetPosition()).Run()
		radix.NewSync((remoteHashTree)(nextSuccessor), self.tree).From(self.node.GetPredecessor().Pos).To(self.node.GetPosition()).Run()
		nextSuccessor = self.node.GetSuccessor(nextSuccessor.Pos)
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
func (self *DHash) clean() {
	fmt.Println("implement clean!")
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
func (self *DHash) AddTopologyListener(listener discord.TopologyListener) {
	self.node.AddTopologyListener(listener)
}
func (self *DHash) DescribeTree() string {
	return self.tree.Describe()
}
func (self *DHash) Get(data common.Item, result *common.Item) error {
	*result = data
	if value, timestamp, exists := self.tree.Get(data.Key); exists {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			result.Exists = true
			result.Value = []byte(byteHasher)
			result.Timestamp = timestamp
		}
	}
	return nil
}
func (self *DHash) Put(data common.Item) error {
	successor := self.node.GetSuccessor(data.Key)
	var x int
	if successor.Addr != self.node.GetAddr() {
		return successor.Call("DHash.Put", data, &x)
	}
	if nodeCount := self.node.CountNodes(); nodeCount < common.Redundancy {
		data.TTL = nodeCount
	} else {
		data.TTL = common.Redundancy
	}
	data.Timestamp = self.timer.ContinuousTime()
	return self.put(data)
}
func (self *DHash) forwardPut(data common.Item) {
	data.TTL--
	successor := self.node.GetSuccessor(self.node.GetPosition())
	var x int
	err := successor.Call("DHash.SlavePut", data, &x)
	for err != nil {
		self.node.RemoveNode(successor)
		successor = self.node.GetSuccessor(self.node.GetPosition())
		err = successor.Call("DHash.SlavePut", data, &x)
	}
}
func (self *DHash) put(data common.Item) error {
	if data.TTL > 1 {
		go self.forwardPut(data)
	}
	self.tree.Put(data.Key, radix.ByteHasher(data.Value), data.Timestamp)
	return nil
}

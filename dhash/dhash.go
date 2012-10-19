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
func (self *DHash) getSuccessor(pos []byte) common.Remote {
	return self.node.GetSuccessor(pos)
}
func (self *DHash) getPredecessor() common.Remote {
	return self.node.GetPredecessor()
}
func (self *DHash) getAddr() string {
	return self.node.GetAddr()
}
func (self *DHash) removeNode(r common.Remote) {
	self.node.RemoveNode(r)
}
func (self *DHash) getPosition() []byte {
	return self.node.GetPosition()
}
func (self *DHash) sync() {
	nextSuccessor := self.getSuccessor(self.getPosition())
	for i := 0; i < common.Redundancy; i++ {
		//		radix.NewSync((*lockTree)(self.tree), (remoteTree)(nextSuccessor)).From(
		nextSuccessor = self.getSuccessor(nextSuccessor.Pos)
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
	self.timer.Conform(remotePeer{Addr: addr})
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
	successor := self.getSuccessor(data.Key)
	var x int
	if successor.Addr != self.getAddr() {
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
	successor := self.getSuccessor(self.getPosition())
	var x int
	err := successor.Call("DHash.SlavePut", data, &x)
	for err != nil {
		self.removeNode(successor)
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

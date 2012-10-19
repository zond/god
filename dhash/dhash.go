package dhash

import (
	"../common"
	"../discord"
	"../radix"
	"../timenet"
	"fmt"
	"sync"
	"time"
)

const (
	syncInterval  = time.Second * 10
	cleanInterval = time.Second * 10
)

type nodeState int

const (
	created = iota
	started
	stopped
)

type DHash struct {
	state nodeState
	lock  *sync.RWMutex
	node  *discord.Node
	timer *timenet.Timer
	tree  *radix.Tree
}

func NewDHash(addr string) (result *DHash) {
	result = &DHash{
		lock:  &sync.RWMutex{},
		node:  discord.NewNode(addr),
		state: created,
		tree:  radix.NewTree(),
	}
	result.timer = timenet.NewTimer((*dhashPeerProducer)(result))
	result.node.Export("Timer", (*timerServer)(result.timer))
	result.node.Export("DHash", (*dhashServer)(result))
	return
}
func (self *DHash) hasState(s nodeState) bool {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.state == s
}
func (self *DHash) changeState(old, neu nodeState) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	if self.state != old {
		return false
	}
	self.state = neu
	return true
}
func (self *DHash) Stop() {
	if self.changeState(started, stopped) {
		self.lock.Lock()
		defer self.lock.Unlock()
		self.node.Stop()
		self.timer.Stop()
	}
}
func (self *DHash) Start() (err error) {
	if !self.changeState(created, started) {
		return fmt.Errorf("%v can only be started when in state 'created'", self)
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	if err = self.node.Start(); err != nil {
		return
	}
	self.timer.Start()
	go self.syncPeriodically()
	go self.cleanPeriodically()
	return
}
func (self *DHash) sync() {
	fmt.Println("implement sync!")
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
func (self *DHash) DescribeTree() string {
	self.lock.RLock()
	defer self.lock.RUnlock()
	fmt.Println("gonna return", self.tree.Describe())
	return self.tree.Describe()
}
func (self *DHash) Get(data common.Item, result *common.Item) error {
	self.lock.RLock()
	defer self.lock.RUnlock()
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
	self.lock.RLock()
	successor := self.node.GetSuccessor(data.Key)
	self.lock.RUnlock()
	var x int
	if successor.Addr != self.node.GetAddr() {
		return successor.Call("DHash.Put", data, &x)
	}
	self.lock.RLock()
	if nodeCount := self.node.CountNodes(); nodeCount < common.Redundancy {
		data.TTL = nodeCount
	} else {
		data.TTL = common.Redundancy
	}
	data.Timestamp = self.timer.ContinuousTime()
	self.lock.RUnlock()
	return self.put(data)
}
func (self *DHash) forwardPut(data common.Item) {
	data.TTL--
	self.lock.RLock()
	successor := self.node.GetSuccessor(self.node.GetPosition())
	self.lock.RUnlock()
	var x int
	err := successor.Call("DHash.SlavePut", data, &x)
	for err != nil {
		self.lock.RLock()
		self.node.RemoveNode(successor)
		successor = self.node.GetSuccessor(self.node.GetPosition())
		self.lock.RUnlock()
		err = successor.Call("DHash.SlavePut", data, &x)
	}
}
func (self *DHash) put(data common.Item) error {
	if data.TTL > 1 {
		go self.forwardPut(data)
	}
	self.lock.Lock()
	defer self.lock.Unlock()
	self.tree.Put(data.Key, radix.ByteHasher(data.Value), data.Timestamp)
	return nil
}

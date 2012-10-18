package discord

import (
	"../common"
	"../murmur"
	"bytes"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"
)

type nodeState int

const (
	created = iota
	started
	stopped
)

type Node struct {
	ring        *common.Ring
	predecessor *common.Remote
	position    []byte
	addr        string
	listener    *net.TCPListener
	lock        *sync.RWMutex
	state       nodeState
	exports     map[string]interface{}
}

func NewNode(addr string) (result *Node) {
	return &Node{
		ring:     &common.Ring{},
		position: make([]byte, murmur.Size),
		addr:     addr,
		exports:  make(map[string]interface{}),
		lock:     new(sync.RWMutex),
		state:    created,
	}
}
func (self *Node) Export(name string, api interface{}) error {
	if self.hasState(created) {
		self.lock.Lock()
		defer self.lock.Unlock()
		self.exports[name] = api
		return nil
	}
	return fmt.Errorf("%v can only export when in state 'created'")
}
func (self *Node) SetPosition(position []byte) *Node {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.position = position
	self.predecessor = nil
	return self
}
func (self *Node) GetRing(ring *common.Ring) {
	*ring = common.Ring{self.GetNodes()}
	return
}
func (self *Node) GetNodes() (result []common.Remote) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	result = make([]common.Remote, len(self.ring.Nodes))
	copy(result, self.ring.Nodes)
	return
}
func (self *Node) CountNodes() (result int) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	result = len(self.ring.Nodes)
	return
}
func (self *Node) GetPosition() (result []byte) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	result = make([]byte, len(self.position))
	copy(result, self.position)
	return
}
func (self *Node) String() string {
	return fmt.Sprintf("<%v@%v predecessor=%v>", common.HexEncode(self.GetPosition()), self.getAddr(), self.getPredecessor())
}
func (self *Node) Describe() string {
	self.lock.RLock()
	defer self.lock.RUnlock()
	buffer := bytes.NewBufferString(fmt.Sprintf("%v@%v predecessor=%v\n", common.HexEncode(self.position), self.addr, self.predecessor))
	fmt.Fprint(buffer, self.ring.Describe())
	return string(buffer.Bytes())
}

func (self *Node) hasState(s nodeState) bool {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.state == s
}
func (self *Node) changeState(old, neu nodeState) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	if self.state != old {
		return false
	}
	self.state = neu
	return true
}
func (self *Node) getPredecessor() *common.Remote {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.predecessor
}
func (self *Node) getListener() *net.TCPListener {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.listener
}
func (self *Node) setListener(l *net.TCPListener) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.listener = l
}
func (self *Node) setAddr(addr string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.addr = addr
}
func (self *Node) getAddr() string {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.addr
}
func (self *Node) remote() common.Remote {
	return common.Remote{self.GetPosition(), self.getAddr()}
}

func (self *Node) Stop() {
	if self.changeState(started, stopped) {
		self.getListener().Close()
	}
}
func (self *Node) MustStart() {
	if err := self.Start(); err != nil {
		panic(err)
	}
}
func (self *Node) Start() (err error) {
	if !self.changeState(created, started) {
		return fmt.Errorf("%v can only be started when in state 'created'")
	}
	if self.getAddr() == "" {
		var foundAddr string
		if foundAddr, err = findAddress(); err != nil {
			return
		}
		self.setAddr(foundAddr)
	}
	var addr *net.TCPAddr
	if addr, err = net.ResolveTCPAddr("tcp", self.getAddr()); err != nil {
		return
	}
	var listener *net.TCPListener
	if listener, err = net.ListenTCP("tcp", addr); err != nil {
		return
	}
	self.setListener(listener)
	server := rpc.NewServer()
	if err = server.RegisterName("Node", (*nodeServer)(self)); err != nil {
		return
	}
	for name, api := range self.exports {
		if err = server.RegisterName(name, api); err != nil {
			return
		}
	}
	selfRemote := self.remote()
	self.lock.Lock()
	self.ring.Add(selfRemote)
	self.lock.Unlock()
	go server.Accept(self.getListener())
	go self.notifyPeriodically()
	go self.pingPeriodically()
	return
}
func (self *Node) notifyPeriodically() {
	for self.hasState(started) {
		self.notifySuccessor()
		time.Sleep(time.Second)
	}
}
func (self *Node) pingPeriodically() {
	for self.hasState(started) {
		self.pingPredecessor()
		time.Sleep(time.Second)
	}
}
func (self *Node) Ping() {
}
func (self *Node) pingPredecessor() {
	if predecessor := self.getPredecessor(); predecessor != nil {
		var x int
		if err := predecessor.Call("Node.Ping", 0, &x); err != nil {
			self.lock.Lock()
			defer self.lock.Unlock()
			predecessor, _, _ = self.ring.Remotes(self.GetPosition())
			self.predecessor = predecessor
		}
	}
}
func (self *Node) notify(caller common.Remote) (result common.Ring) {
	self.lock.Lock()
	self.ring.Add(caller)
	self.predecessor, _, _ = self.ring.Remotes(self.position)
	self.lock.Unlock()
	result.Nodes = self.GetNodes()
	return
}
func (self *Node) notifySuccessor() {
	self.lock.RLock()
	_, _, successor := self.ring.Remotes(self.GetPosition())
	self.lock.RUnlock()
	newRing := &common.Ring{}
	if err := successor.Call("Node.Notify", self.remote(), newRing); err != nil {
		self.RemoveNode(*successor)
		self.notifySuccessor()
	} else {
		self.lock.Lock()
		defer self.lock.Unlock()
		self.ring = newRing
		if self.predecessor != nil {
			self.ring.Add(*self.predecessor)
			self.ring.Clean(self.predecessor.Pos, self.position)
		}
	}
}
func (self *Node) MustJoin(addr string) {
	if err := self.Join(addr); err != nil {
		panic(err)
	}
}
func (self *Node) Join(addr string) (err error) {
	if bytes.Compare(self.GetPosition(), make([]byte, murmur.Size)) == 0 {
		newRing := &common.Ring{}
		if err = common.Switch.Call(addr, "Node.Ring", 0, newRing); err != nil {
			return
		}
		self.SetPosition(newRing.GetSlot())
	}
	newRing := &common.Ring{}
	if err = common.Switch.Call(addr, "Node.Notify", self.remote(), &newRing); err != nil {
		return
	}
	self.lock.Lock()
	defer self.lock.Unlock()
	self.ring = newRing
	return
}
func (self *Node) RemoveNode(remote common.Remote) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.ring.Remove(remote)
}
func (self *Node) GetSuccessor(key []byte) common.Remote {
	self.lock.RLock()
	predecessor, match, successor := self.ring.Remotes(key)
	self.lock.RUnlock()
	if match != nil {
		predecessor = match
	}
	if predecessor.Addr != self.getAddr() {
		if err := predecessor.Call("Node.GetSuccessor", key, successor); err != nil {
			self.RemoveNode(*successor)
			return self.GetSuccessor(key)
		}
	}
	return *successor
}

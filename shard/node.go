package shard

import (
	"../murmur"
	"bytes"
	"fmt"
	"math/rand"
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
	ring        *Ring
	predecessor *Remote
	position    []byte
	addr        string
	listener    *net.TCPListener
	lock        *sync.RWMutex
	state       nodeState
}

func NewNode(addr string) (result *Node) {
	return &Node{
		ring:     &Ring{},
		position: murmur.HashInt64(rand.Int63()),
		addr:     addr,
		lock:     new(sync.RWMutex),
		state:    created,
	}
}
func (self *Node) SetPosition(position []byte) *Node {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.position = position
	return self
}
func (self *Node) String() string {
	return fmt.Sprintf("<%v@%v predecessor=%v>", hexEncode(self.getPosition()), self.getAddr(), self.getPredecessor())
}
func (self *Node) Describe() string {
	self.lock.RLock()
	defer self.lock.RUnlock()
	buffer := bytes.NewBufferString(fmt.Sprintf("%v@%v predecessor=%v\n", hexEncode(self.position), self.addr, self.predecessor))
	self.ring.describe(buffer)
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
func (self *Node) getPredecessor() *Remote {
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
func (self *Node) getPosition() (result []byte) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.position[:]
}
func (self *Node) remote() Remote {
	return Remote{self.getPosition(), self.getAddr()}
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
	selfRemote := self.remote()
	self.lock.Lock()
	self.ring.add(selfRemote)
	self.predecessor = &selfRemote
	self.lock.Unlock()
	go server.Accept(self.getListener())
	go self.notifyPeriodically()
	return
}
func (self *Node) notifyPeriodically() {
	for self.hasState(started) {
		self.notifySuccessor()
		time.Sleep(time.Second)
	}
}
func (self *Node) notify(caller Remote, ring *Ring) error {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.ring.add(caller)
	self.predecessor, _, _ = self.ring.remotes(self.position)
	*ring = *self.ring
	return nil
}
func (self *Node) notifySuccessor() (err error) {
	self.lock.RLock()
	_, _, successor := self.ring.remotes(self.getPosition())
	self.lock.RUnlock()
	newRing := &Ring{}
	if err = successor.call("Node.Notify", self.remote(), newRing); err != nil {
		return
	}
	self.lock.Lock()
	defer self.lock.Unlock()
	self.ring = newRing
	if self.predecessor != nil {
		self.ring.add(*self.predecessor)
		self.ring.clean(self.predecessor.Pos, self.position)
	}
	return
}
func (self *Node) MustJoin(addr string) {
	if err := self.Join(addr); err != nil {
		panic(err)
	}
}
func (self *Node) Join(addr string) (err error) {
	if err = board.call(addr, "Node.Notify", self.remote(), &self.ring); err != nil {
		return
	}
	return
}

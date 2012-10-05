package shard

import (
	"fmt"
	"net"
	"net/rpc"
	"sync"
)

type Node struct {
	nodes    Remotes
	position []byte
	addr     string
	listener *net.TCPListener
	lock     *sync.RWMutex
}

func NewNode(addr string) (result *Node) {
	return &Node{
		nodes: Remotes{},
		addr:  addr,
		lock:  new(sync.RWMutex),
	}
}
func (self *Node) SetPosition(position []byte) *Node {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.position = position
	return self
}
func (self *Node) String() string {
	return fmt.Sprintf("<%v@%v predecessor=%v successor=%v>", hexEncode(self.getPosition()), self.getAddr(), self.getPredecessor(), self.getSuccessor())
}

func (self *Node) getSuccessor() Remote {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.nodes.surrounding(self.position).Successor
}
func (self *Node) getPredecessor() Remote {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.nodes.surrounding(self.position).Predecessor
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
	result = make([]byte, len(self.position))
	copy(result, self.position)
	return
}
func (self *Node) remote() Remote {
	return Remote{self.getPosition(), self.getAddr()}
}

func (self *Node) Stop() {
	self.getListener().Close()
}
func (self *Node) MustStart() {
	if err := self.Start(); err != nil {
		panic(err)
	}
}
func (self *Node) Start() (err error) {
	if self.getAddr() == "" {
		var udpAddr *net.UDPAddr
		if udpAddr, err = net.ResolveUDPAddr("udp", "www.internic.net:80"); err != nil {
			return
		}
		var udpConn *net.UDPConn
		if udpConn, err = net.DialUDP("udp", nil, udpAddr); err != nil {
			return
		}
		self.setAddr(udpConn.LocalAddr().String())
	}
	self.nodes.add(self.remote())
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
	fmt.Println(self, "listening on", self.getAddr())
	go server.Accept(self.getListener())
	return
}
func (self *Node) findSurrounding(position []byte, result *Surrounding) (err error) {
	*result = self.nodes.surrounding(position)
	if result.Predecessor.Addr != self.getAddr() {
		err = result.Predecessor.call("Node.FindSurrounding", position, result)
	}
	return
}
func (self *Node) notify(caller Remote, nodes *Remotes) error {
	self.nodes.add(caller)
	*nodes = self.nodes
	return nil
}
func (self *Node) MustJoin(addr string) {
	if err := self.Join(addr); err != nil {
		panic(err)
	}
}
func (self *Node) Join(addr string) (err error) {
	if err = board.call(addr, "Node.Notify", self.remote(), &self.nodes); err != nil {
		return
	}
	return
}

package discord

import (
	"../common"
	"../murmur"
	"bytes"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"
)

type CommListener func(source, dest common.Remote, typ string) bool

type PingPack struct {
	Caller   common.Remote
	RingHash []byte
}

const (
	created = iota
	started
	stopped
)

type Node struct {
	ring          *common.Ring
	position      []byte
	addr          string
	listener      *net.TCPListener
	metaLock      *sync.RWMutex
	routeLock     *sync.Mutex
	state         int32
	exports       map[string]interface{}
	commListeners []CommListener
}

func NewNode(addr string) (result *Node) {
	return &Node{
		ring:      common.NewRing(),
		position:  make([]byte, murmur.Size),
		addr:      addr,
		exports:   make(map[string]interface{}),
		metaLock:  new(sync.RWMutex),
		routeLock: new(sync.Mutex),
		state:     created,
	}
}
func (self *Node) Export(name string, api interface{}) error {
	if self.hasState(created) {
		self.metaLock.Lock()
		defer self.metaLock.Unlock()
		self.exports[name] = api
		return nil
	}
	return fmt.Errorf("%v can only export when in state 'created'")
}
func (self *Node) AddCommListener(f CommListener) {
	self.metaLock.Lock()
	defer self.metaLock.Unlock()
	self.commListeners = append(self.commListeners, f)
}
func (self *Node) triggerCommListeners(source, dest common.Remote, typ string) {
	self.metaLock.RLock()
	newListeners := make([]CommListener, 0, len(self.commListeners))
	for _, l := range self.commListeners {
		self.metaLock.RUnlock()
		if l(source, dest, typ) {
			newListeners = append(newListeners, l)
		}
		self.metaLock.RLock()
	}
	self.metaLock.RUnlock()
	self.metaLock.Lock()
	defer self.metaLock.Unlock()
	self.commListeners = newListeners
}
func (self *Node) AddChangeListener(f common.RingChangeListener) {
	self.ring.AddChangeListener(f)
}
func (self *Node) SetPosition(position []byte) *Node {
	self.metaLock.Lock()
	self.position = make([]byte, len(position))
	copy(self.position, position)
	self.metaLock.Unlock()
	self.routeLock.Lock()
	defer self.routeLock.Unlock()
	self.ring.Add(self.Remote())
	return self
}
func (self *Node) GetNodes() (result common.Remotes) {
	return self.ring.Nodes()
}
func (self *Node) Redundancy() int {
	return self.ring.Redundancy()
}
func (self *Node) CountNodes() int {
	return self.ring.Size()
}
func (self *Node) GetPosition() (result []byte) {
	self.metaLock.RLock()
	defer self.metaLock.RUnlock()
	result = make([]byte, len(self.position))
	copy(result, self.position)
	return
}
func (self *Node) GetAddr() string {
	self.metaLock.RLock()
	defer self.metaLock.RUnlock()
	return self.addr
}
func (self *Node) String() string {
	return fmt.Sprintf("<%v@%v>", common.HexEncode(self.GetPosition()), self.GetAddr())
}
func (self *Node) Describe() string {
	self.metaLock.RLock()
	buffer := bytes.NewBufferString(fmt.Sprintf("%v@%v\n", common.HexEncode(self.position), self.addr))
	self.metaLock.RUnlock()
	fmt.Fprint(buffer, self.ring.Describe())
	return string(buffer.Bytes())
}

func (self *Node) hasState(s int32) bool {
	return atomic.LoadInt32(&self.state) == s
}
func (self *Node) changeState(old, neu int32) bool {
	return atomic.CompareAndSwapInt32(&self.state, old, neu)
}
func (self *Node) getListener() *net.TCPListener {
	self.metaLock.RLock()
	defer self.metaLock.RUnlock()
	return self.listener
}
func (self *Node) setListener(l *net.TCPListener) {
	self.metaLock.Lock()
	defer self.metaLock.Unlock()
	self.listener = l
}
func (self *Node) setAddr(addr string) {
	self.metaLock.Lock()
	defer self.metaLock.Unlock()
	self.addr = addr
}
func (self *Node) Remote() common.Remote {
	return common.Remote{self.GetPosition(), self.GetAddr()}
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
		return fmt.Errorf("%v can only be started when in state 'created'", self)
	}
	if self.GetAddr() == "" {
		var foundAddr string
		if foundAddr, err = findAddress(); err != nil {
			return
		}
		self.setAddr(foundAddr)
	}
	var addr *net.TCPAddr
	if addr, err = net.ResolveTCPAddr("tcp", self.GetAddr()); err != nil {
		return
	}
	var listener *net.TCPListener
	if listener, err = net.ListenTCP("tcp", addr); err != nil {
		return
	}
	self.setListener(listener)
	server := rpc.NewServer()
	if err = server.RegisterName("Discord", (*nodeServer)(self)); err != nil {
		return
	}
	for name, api := range self.exports {
		if err = server.RegisterName(name, api); err != nil {
			return
		}
	}
	self.ring.Add(self.Remote())
	go server.Accept(self.getListener())
	go self.notifyPeriodically()
	go self.pingPeriodically()
	return
}
func (self *Node) notifyPeriodically() {
	for self.hasState(started) {
		self.notifySuccessor()
		time.Sleep(common.PingInterval)
	}
}
func (self *Node) pingPeriodically() {
	for self.hasState(started) {
		self.pingPredecessor()
		time.Sleep(common.PingInterval)
	}
}
func (self *Node) RingHash() []byte {
	return self.ring.Hash()
}
func (self *Node) Ping(ping PingPack) (me common.Remote) {
	me = self.Remote()
	if bytes.Compare(ping.RingHash, self.ring.Hash()) != 0 {
		var newNodes common.Remotes
		if err := ping.Caller.Call("Discord.Nodes", 0, &newNodes); err != nil {
			self.RemoveNode(ping.Caller)
		} else {
			self.routeLock.Lock()
			defer self.routeLock.Unlock()
			pred := self.ring.Predecessor(me)
			self.ring.SetNodes(newNodes)
			self.ring.Add(me)
			self.ring.Add(pred)
			self.ring.Clean(pred, me)
		}
	}
	return
}
func (self *Node) pingPredecessor() {
	pred := self.GetPredecessor()
	ping := PingPack{
		RingHash: self.ring.Hash(),
		Caller:   self.Remote(),
	}
	var newPred common.Remote
	op := "Discord.Ping"
	self.triggerCommListeners(self.Remote(), pred, op)
	if err := pred.Call(op, ping, &newPred); err != nil {
		self.RemoveNode(pred)
	} else {
		self.routeLock.Lock()
		defer self.routeLock.Unlock()
		self.ring.Add(newPred)
	}
}
func (self *Node) Nodes() common.Remotes {
	return self.ring.Nodes()
}
func (self *Node) Notify(caller common.Remote) common.Remote {
	self.routeLock.Lock()
	defer self.routeLock.Unlock()
	self.ring.Add(caller)
	return self.GetPredecessor()
}
func (self *Node) notifySuccessor() {
	succ := self.GetSuccessor()
	var otherPred common.Remote
	op := "Discord.Notify"
	selfRemote := self.Remote()
	self.triggerCommListeners(selfRemote, succ, op)
	if err := succ.Call(op, selfRemote, &otherPred); err != nil {
		self.RemoveNode(succ)
	} else {
		if otherPred.Addr != self.GetAddr() {
			self.routeLock.Lock()
			defer self.routeLock.Unlock()
			self.ring.Add(otherPred)
		}
	}
}
func (self *Node) MustJoin(addr string) {
	if err := self.Join(addr); err != nil {
		panic(err)
	}
}
func (self *Node) Join(addr string) (err error) {
	var newNodes common.Remotes
	if err = common.Switch.Call(addr, "Discord.Nodes", 0, &newNodes); err != nil {
		return
	}
	if bytes.Compare(self.GetPosition(), make([]byte, murmur.Size)) == 0 {
		self.SetPosition(common.NewRingNodes(newNodes).GetSlot())
	}
	self.routeLock.Lock()
	self.ring.SetNodes(newNodes)
	self.routeLock.Unlock()
	var x common.Remote
	if err = common.Switch.Call(addr, "Discord.Notify", self.Remote(), &x); err != nil {
		return
	}
	return
}
func (self *Node) RemoveNode(remote common.Remote) {
	if remote.Addr == self.GetAddr() {
		panic(fmt.Errorf("%v is trying to remove itself from the routing!", self))
	}
	self.routeLock.Lock()
	defer self.routeLock.Unlock()
	self.ring.Remove(remote)
}
func (self *Node) GetPredecessor() common.Remote {
	return self.GetPredecessorForRemote(self.Remote())
}
func (self *Node) GetPredecessorForRemote(r common.Remote) common.Remote {
	return self.ring.Predecessor(r)
}
func (self *Node) GetPredecessorFor(key []byte) common.Remote {
	pred, _, _ := self.ring.Remotes(key)
	return *pred
}
func (self *Node) HasNode(pos []byte) bool {
	if _, match, _ := self.ring.Remotes(pos); match != nil {
		return true
	}
	return false
}
func (self *Node) GetSuccessor() common.Remote {
	return self.GetSuccessorForRemote(self.Remote())
}
func (self *Node) GetSuccessorForRemote(r common.Remote) common.Remote {
	return self.ring.Successor(r)
}
func (self *Node) GetSuccessorFor(key []byte) common.Remote {
	// Guess according to our route cache
	predecessor, match, successor := self.ring.Remotes(key)
	if match != nil {
		predecessor = match
	}
	// If we consider ourselves successors, just return us
	if successor.Addr != self.GetAddr() {
		// Double check by asking the successor we found what predecessor it has
		if err := successor.Call("Discord.GetPredecessor", 0, predecessor); err != nil {
			self.RemoveNode(*successor)
			return self.GetSuccessorFor(key)
		}
		// If the key we are looking for is between them, just return the successor
		if !common.BetweenIE(key, predecessor.Pos, successor.Pos) {
			// Otherwise, ask the predecessor we actually found about who is the successor of the key
			if err := predecessor.Call("Discord.GetSuccessorFor", key, successor); err != nil {
				self.RemoveNode(*predecessor)
				return self.GetSuccessorFor(key)
			}
		}
	}
	return *successor
}

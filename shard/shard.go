package shard

import (
	"../murmur"
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func hexEncode(b []byte) (result string) {
	encoded := hex.EncodeToString(b)
	buffer := new(bytes.Buffer)
	for i := len(encoded); i < len(b)*2; i++ {
		fmt.Fprint(buffer, "00")
	}
	fmt.Fprint(buffer, encoded)
	return string(buffer.Bytes())
}

func between(needle, start, end []byte) (result bool) {
	switch bytes.Compare(start, end) {
	case 0:
		result = true
	case -1:
		result = bytes.Compare(start, needle) < 1 && bytes.Compare(needle, end) < 0
	case 1:
		result = bytes.Compare(start, needle) < 1 || bytes.Compare(needle, end) < 0
	default:
		panic("Shouldn't happen")
	}
	return
}

var board = newSwitchboard()

type Remote struct {
	Id       []byte
	Position []byte
	Addr     string
}

func (self Remote) String() string {
	return fmt.Sprintf("[%v @ %v : %v]", hexEncode(self.Id), self.Addr, hexEncode(self.Position))
}
func (self Remote) between(start, end []byte) bool {
	return between(self.Position, start, end)
}
func (self Remote) call(service string, args, reply interface{}) error {
	return board.call(self.Addr, service, args, reply)
}

type shardServer Shard

func (self *shardServer) Predecessor(x int, result *Remote) error {
	*result = (*Shard)(self).getPredecessor()
	return nil
}
func (self *shardServer) Successor(x int, result *Remote) error {
	*result = (*Shard)(self).getSuccessor()
	return nil
}
func (self *shardServer) FindSuccessor(position []byte, result *Remote) error {
	return (*Shard)(self).findSuccessor(position, result)
}
func (self *shardServer) ClosestPrecedingFinger(position []byte, result *Remote) error {
	return (*Shard)(self).closestPrecedingFinger(position, result)
}

type Shard struct {
	finger      []Remote
	address     string
	listener    *net.TCPListener
	id          []byte
	position    []byte
	lock        *sync.RWMutex
	predecessor Remote
}

func NewShard(addr string) *Shard {
	return &Shard{
		position: make([]byte, murmur.Size),
		lock:     new(sync.RWMutex),
		finger:   make([]Remote, 8*murmur.Size),
		id:       murmur.HashInt64(rand.Int63()),
		address:  addr,
	}
}
func (self *Shard) SetPosition(position []byte) *Shard {
	self.position = position
	return self
}
func (self *Shard) String() string {
	return fmt.Sprintf("<%v @ %v : %v predecessor = %v>", hexEncode(self.getId()), self.getAddress(), hexEncode(self.getPosition()), self.getPredecessor())
}

func (self *Shard) getSuccessor() Remote {
	return self.getFinger(1)
}
func (self *Shard) setSuccessor(r Remote) {
	self.setFinger(1, r)
}
func (self *Shard) getPredecessor() Remote {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.predecessor
}
func (self *Shard) setPredecessor(p Remote) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.predecessor = p
}
func (self *Shard) getListener() *net.TCPListener {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.listener
}
func (self *Shard) setListener(l *net.TCPListener) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.listener = l
}
func (self *Shard) getAddress() string {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.address
}
func (self *Shard) getFinger(i int) Remote {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.finger[i]
}
func (self *Shard) setFinger(i int, r Remote) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.finger[i] = r
}
func (self *Shard) getId() (result []byte) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	result = make([]byte, len(self.id))
	copy(result, self.id)
	return
}
func (self *Shard) getPosition() (result []byte) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	result = make([]byte, len(self.position))
	copy(result, self.position)
	return
}
func (self *Shard) remote() Remote {
	return Remote{self.getId(), self.getPosition(), self.getAddress()}
}

func (self *Shard) Stop() {
	self.getListener().Close()
}
func (self *Shard) MustStart() {
	if err := self.Start(); err != nil {
		panic(err)
	}
}
func (self *Shard) Start() (err error) {
	var addr *net.TCPAddr
	if addr, err = net.ResolveTCPAddr("tcp", self.getAddress()); err != nil {
		return
	}
	var listener *net.TCPListener
	if listener, err = net.ListenTCP("tcp", addr); err != nil {
		return
	}
	self.setListener(listener)
	server := rpc.NewServer()
	if err = server.RegisterName("Shard", (*shardServer)(self)); err != nil {
		return
	}
	selfRemote := self.remote()
	for i := 1; i < len(self.finger); i++ {
		self.finger[i] = selfRemote
	}
	self.setPredecessor(selfRemote)
	go server.Accept(self.getListener())
	return
}
func (self *Shard) findSuccessor(position []byte, result *Remote) (err error) {
	if err = self.findPredecessor(position, result); err != nil {
		return
	}
	if bytes.Compare(result.Id, self.getId()) == 0 {
		*result = self.getPredecessor()
	} else {
		err = result.call("Shard.Predecessor", 0, result)
	}
	return
}
func (self *Shard) findPredecessor(position []byte, result *Remote) (err error) {
	if err = self.closestPrecedingFinger(position, result); err != nil {
		return
	}
	var resultSuccessor Remote
	if err = result.call("Shard.Successor", 0, &resultSuccessor); err != nil {
		return
	}
	for {
		if between(position, result.Position, resultSuccessor.Position) {
			return
		}
		if err = result.call("Shard.ClosestPrecedingFinger", position, result); err != nil {
			return
		}
	}
	*result = self.remote()
	return
}
func (self *Shard) closestPrecedingFinger(position []byte, result *Remote) (err error) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	for i := len(self.finger) - 1; i > 0; i-- {
		if self.finger[i].between(self.id, position) {
			*result = self.finger[i]
			return
		}
	}
	*result = self.remote()
	return
}
func (self *Shard) initFingerTable(address string) (err error) {
	var successor Remote
	if err = board.call(address, "Shard.FindSuccessor", self.getPosition(), &successor); err != nil {
		return
	}
	self.setSuccessor(successor)
	fmt.Println(self, self.getSuccessor())
	return
}
func (self *Shard) MustJoin(address string) {
	if err := self.Join(address); err != nil {
		panic(err)
	}
}
func (self *Shard) Join(address string) (err error) {
	if err = self.initFingerTable(address); err != nil {
		return
	}
	return
}

package shard

import (
	"../murmur"
	"fmt"
	"net"
	"net/rpc"
)

var board = newSwitchboard()

type remote struct {
	id       []byte
	position []byte
	addr     string
}

func (self *remote) call(service string, args, reply interface{}) error {
	return board.call(self.addr, service, args, reply)
}

type shardServer Shard

func (self *shardServer) Ping(a int, b *int) error {
	fmt.Println("ping!")
	return nil
}

type Shard struct {
	finger   []*remote
	address  string
	listener *net.TCPListener
}

func NewShard(addr string) (result *Shard) {
	return &Shard{
		finger:  make([]*remote, 8*murmur.Size),
		address: addr,
	}
}
func (self *Shard) Stop() {
	self.listener.Close()
}
func (self *Shard) Start() (err error) {
	var addr *net.TCPAddr
	if addr, err = net.ResolveTCPAddr("tcp", self.address); err != nil {
		return
	}
	if self.listener, err = net.ListenTCP("tcp", addr); err != nil {
		return
	}
	rpc.RegisterName("Shard", (*shardServer)(self))
	go rpc.Accept(self.listener)
	return
}
func (self *Shard) Join(address string) {
	a := 0
	board.call(address, "Shard.Ping", a, &a)
}

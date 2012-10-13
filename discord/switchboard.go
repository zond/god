package discord

import (
	"net/rpc"
	"sync"
)

var board = newSwitchboard()

type switchboard struct {
	lock    *sync.RWMutex
	clients map[string]*rpc.Client
}

func newSwitchboard() *switchboard {
	return &switchboard{new(sync.RWMutex), make(map[string]*rpc.Client)}
}
func (self *switchboard) call(addr, service string, args, reply interface{}) (err error) {
	self.lock.RLock()
	client, ok := self.clients[addr]
	self.lock.RUnlock()
	if !ok {
		if client, err = rpc.Dial("tcp", addr); err != nil {
			return
		}
		self.lock.Lock()
		self.clients[addr] = client
		self.lock.Unlock()
	}
	err = client.Call(service, args, reply)
	return
}

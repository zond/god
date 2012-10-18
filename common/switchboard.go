package common

import (
	"net/rpc"
	"sync"
)

var Switch = newSwitchboard()

type Switchboard struct {
	lock    *sync.RWMutex
	clients map[string]*rpc.Client
}

func newSwitchboard() *Switchboard {
	return &Switchboard{new(sync.RWMutex), make(map[string]*rpc.Client)}
}
func (self *Switchboard) client(addr string) (client *rpc.Client, err error) {
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
	return
}
func (self *Switchboard) Call(addr, service string, args, reply interface{}) (err error) {
	var client *rpc.Client
	if client, err = self.client(addr); err != nil {
		return
	}
	if err = client.Call(service, args, reply); err != nil {
		if err.Error() == "connection is shut down" {
			self.lock.Lock()
			delete(self.clients, addr)
			self.lock.Unlock()
		}
		err = self.Call(addr, service, args, reply)
	}
	return
}

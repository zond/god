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
func (self *switchboard) client(addr) (client *rpc.Client, err error) {
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
func (self *switchboard) acall(addr, service string, args, reply interface{}) (call *rpc.Call) {
	if client, err := self.client(addr); err != nil {
		call = &rpc.Call{
			ServiceMethod: service,
			Args:          args,
			Reply:         reply,
			Error:         err,
			Done:          make(chan *rpc.Call, 1),
		}
		call.Done <- call
	} else {
		call = client.Go(service, args, reply, nil)
	}
	return
}
func (self *switchboard) call(addr, service string, args, reply interface{}) (err error) {
	if client, err := self.client(addr); err != nil {
		return
	}
	err = client.Call(service, args, reply)
	return
}

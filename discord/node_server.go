package discord

import (
	"../common"
)

type nodeServer Node

func (self *nodeServer) Notify(caller common.Remote, nodes *common.Remotes) error {
	*nodes = (*Node)(self).Notify(caller)
	return nil
}
func (self *nodeServer) Ring(x int, nodes *common.Remotes) error {
	*nodes = (*Node)(self).GetNodes()
	return nil
}
func (self *nodeServer) Ping(x int, y *int) error {
	(*Node)(self).Ping()
	return nil
}
func (self *nodeServer) GetSuccessor(key []byte, successor *common.Remote) error {
	*successor = (*Node)(self).GetSuccessor(key)
	return nil
}

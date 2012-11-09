package discord

import (
	"../common"
)

type nodeServer Node

func (self *nodeServer) Notify(caller common.Remote, ringHash *[]byte) error {
	*ringHash = (*Node)(self).Notify(caller)
	return nil
}
func (self *nodeServer) Nodes(x int, nodes *common.Remotes) error {
	*nodes = (*Node)(self).GetNodes()
	return nil
}
func (self *nodeServer) Ping(x int, pos *[]byte) error {
	return (*Node)(self).Ping(x, pos)
}
func (self *nodeServer) GetPredecessor(x int, predecessor *common.Remote) error {
	*predecessor = (*Node)(self).GetPredecessor()
	return nil
}
func (self *nodeServer) GetSuccessorFor(key []byte, successor *common.Remote) error {
	*successor = (*Node)(self).GetSuccessorFor(key)
	return nil
}

package shard

type nodeServer Node

func (self *nodeServer) Notify(caller Remote, nodes *Ring) error {
	return (*Node)(self).notify(caller, nodes)
}

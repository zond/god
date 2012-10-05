package shard

type nodeServer Node

func (self *nodeServer) FindSurrounding(position []byte, result *Surrounding) error {
	return (*Node)(self).findSurrounding(position, result)
}
func (self *nodeServer) Notify(caller Remote, nodes *Remotes) error {
	return (*Node)(self).notify(caller, nodes)
}

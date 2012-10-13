package shard

type nodeServer Node

func (self *nodeServer) Notify(caller Remote, nodes *Ring) error {
	return (*Node)(self).notify(caller, nodes)
}
func (self *nodeServer) Ring(x int, nodes *Ring) error {
	(*Node)(self).getRing(nodes)
	return nil
}
func (self *nodeServer) Ping(x int, y *int) error {
	(*Node)(self).ping()
	return nil
}

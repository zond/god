package shard

type nodeServer Node

func (self *nodeServer) FindSegment(position []byte, result *Segment) error {
	return (*Node)(self).findSegment(position, result)
}
func (self *nodeServer) Notify(caller Remote, nodes *Ring) error {
	return (*Node)(self).notify(caller, nodes)
}

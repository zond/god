package discord

type nodeServer Node

func (self *nodeServer) Notify(caller Remote, nodes *Ring) error {
	*nodes = (*Node)(self).notify(caller)
	return nil
}
func (self *nodeServer) Ring(x int, nodes *Ring) error {
	(*Node)(self).getRing(nodes)
	return nil
}
func (self *nodeServer) Ping(x int, y *int) error {
	(*Node)(self).ping()
	return nil
}
func (self *nodeServer) GetSuccessor(key []byte, successor *Remote) error {
	*successor = (*Node)(self).GetSuccessor(key)
	return nil
}

package dhash

import (
	"../common"
	"../timenet"
	"time"
)

type remotePeer common.Remote

func (self remotePeer) ActualTime() (result int64) {
	if err := (common.Remote)(self).Call("Timer.ActualTime", 0, &result); err != nil {
		result = time.Now().UnixNano()
	}
	return
}

type dhashPeerProducer DHash

func (self *dhashPeerProducer) Peers() (result map[string]timenet.Peer) {
	result = make(map[string]timenet.Peer)
	for _, node := range (*DHash)(self).node.GetNodes() {
		result[node.Addr] = (remotePeer)(node)
	}
	return
}

type timerServer timenet.Timer

func (self *timerServer) ActualTime(x int, result *int64) error {
	*result = (*timenet.Timer)(self).ActualTime()
	return nil
}

type dhashServer DHash

func (self *dhashServer) SlavePut(data common.Item, x *int) error {
	return (*DHash)(self).put(data)
}
func (self *dhashServer) Put(data common.Item, x *int) error {
	return (*DHash)(self).Put(data)
}
func (self *dhashServer) Get(data common.Item, result *common.Item) error {
	return (*DHash)(self).Get(data, result)
}
func (self *dhashServer) DescribeTree(x int, result *string) error {
	*result = (*DHash)(self).DescribeTree()
	return nil
}

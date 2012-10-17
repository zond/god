package dhash

import (
	"../discord"
	"../timenet"
	"time"
)

type remotePeer discord.Remote

func (self remotePeer) ActualTime() (result int64) {
	if err := (discord.Remote)(self).Call("Timer.ActualTime", 0, &result); err != nil {
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

func (self *timerServer) ActualTime(x int, result *int64) (err error) {
	*result = (*timenet.Timer)(self).ActualTime()
	return
}

type DHash struct {
	node  *discord.Node
	timer *timenet.Timer
}

func NewDHash(addr string) (result *DHash) {
	result = &DHash{
		node: discord.NewNode(addr),
	}
	result.timer = timenet.NewTimer((*dhashPeerProducer)(result))
	result.node.Export("Timer", (*timerServer)(result.timer))
	return
}
func (self *DHash) MustStart() *DHash {
	self.node.MustStart()
	self.timer.Start()
	return self
}
func (self *DHash) MustJoin(addr string) {
	self.timer.Conform(remotePeer{Addr: addr})
	self.node.MustJoin(addr)
}
func (self *DHash) Time() time.Time {
	return time.Unix(0, self.timer.ContinuousTime())
}

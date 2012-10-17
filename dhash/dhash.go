package dhash

import (
	"../discord"
	"../radix"
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

func (self *timerServer) ActualTime(x int, result *int64) error {
	*result = (*timenet.Timer)(self).ActualTime()
	return nil
}

type Item struct {
	Key       []byte
	Value     radix.Hasher
	Exists    bool
	Timestamp int64
}

type dhashServer DHash

func (self *dhashServer) SlavePut(data Item, res *Item) error {
	*res = data
	res.Value, res.Exists = (*DHash)(self).tree.Put(data.Key, data.Value, data.Timestamp)
	return nil
}

type DHash struct {
	node  *discord.Node
	timer *timenet.Timer
	tree  *radix.Tree
}

func NewDHash(addr string) (result *DHash) {
	result = &DHash{
		node: discord.NewNode(addr),
		tree: radix.NewTree(),
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
func (self *DHash) Put(key []byte, value radix.Hasher) (old radix.Hasher, existed bool) {
	timestamp := self.timer.ContinuousTime()
	return self.tree.Put(key, value, timestamp)
}

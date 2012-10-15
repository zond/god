package timenet

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

const (
	loglen         = 10
	dilationFactor = 10
)

type Peer interface {
	ActualTime() (time int64)
}

type PeerProducer interface {
	Peers() map[string]Peer
}

type Timer struct {
	lock          *sync.RWMutex
	offset        int64
	dilations     *dilations
	peerProducer  PeerProducer
	peerLatencies map[string]times
}

func NewTimer(producer PeerProducer) *Timer {
	return &Timer{
		lock:          &sync.RWMutex{},
		peerProducer:  producer,
		dilations:     &dilations{},
		peerLatencies: make(map[string]times),
	}
}
func (self *Timer) adjustments() int64 {
	return self.offset + self.dilations.delta()
}
func (self *Timer) ActualTime() int64 {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return time.Now().UnixNano() + self.adjustments()
}
func (self *Timer) ContinuousTime() int64 {
	self.lock.RLock()
	defer self.lock.RUnlock()
	temporaryEffect, permanentEffect := self.dilations.effect()
	self.offset += permanentEffect
	return time.Now().UnixNano() + self.offset + temporaryEffect
}
func (self *Timer) adjust(adjustment int64) {
	self.dilations.add(adjustment)
}
func (self *Timer) randomPeer() (id string, peer Peer, peerLatencies times) {
	currentPeers := self.peerProducer.Peers()
	chosenIndex := rand.Int() % len(currentPeers)
	newPeerLatencies := make(map[string]times)
	for thisId, thisPeer := range currentPeers {
		if theseLatencies, ok := self.peerLatencies[thisId]; ok {
			newPeerLatencies[thisId] = theseLatencies
			if chosenIndex < 1 {
				peerLatencies = theseLatencies
			}
		}
		if chosenIndex < 1 {
			peer = thisPeer
			id = thisId
		} else {
			chosenIndex--
		}
	}
	self.peerLatencies = newPeerLatencies
	return
}
func (self *Timer) timeAndLatency(peer Peer) (peerTime, latency, myTime int64) {
	latency = -time.Now().UnixNano()
	peerTime = peer.ActualTime()
	latency += time.Now().UnixNano()
	self.lock.RLock()
	defer self.lock.RUnlock()
	myTime = self.ActualTime()
	peerTime += latency / 2
	return
}
func (self *Timer) Conform(peer Peer) {
	peerTime, _, myTime := self.timeAndLatency(peer)
	self.lock.Lock()
	defer self.lock.Unlock()
	self.offset += (peerTime - myTime)
}
func (self *Timer) Sample() {
	self.lock.RLock()
	id, peer, oldLatencies := self.randomPeer()
	self.lock.RUnlock()

	peerTime, latency, myTime := self.timeAndLatency(peer)

	self.lock.Lock()
	defer self.lock.Unlock()
	oldestLatencyIndex := 0
	if len(oldLatencies) > loglen {
		oldestLatencyIndex = len(oldLatencies) - loglen
	}
	newLatencies := append(oldLatencies[oldestLatencyIndex:], latency)
	self.peerLatencies[id] = newLatencies

	mean, deviation := newLatencies.stats()
	if math.Abs(float64(latency-mean)) < float64(deviation) {
		self.adjust(peerTime - myTime)
	}
}
func (self *Timer) sleep() {
	time.Sleep(time.Second)
}
func (self *Timer) Start() {
	for {
		self.Sample()
		self.sleep()
	}
}
func (self *Timer) Run() {
	go self.Start()
}

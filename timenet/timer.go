package timenet

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

const (
	loglen = 10
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
	peerErrors    map[string]int64
	peerLatencies map[string]times
}

func NewTimer(producer PeerProducer) *Timer {
	return &Timer{
		lock:          &sync.RWMutex{},
		peerProducer:  producer,
		dilations:     &dilations{},
		peerErrors:    make(map[string]int64),
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
func (self *Timer) Error() (err int64) {
	if len(self.peerErrors) > 1 {
		var thisErr int64
		for _, e := range self.peerErrors {
			thisErr = e >> 10
			err += thisErr * thisErr
		}
		err = int64(math.Sqrt(float64(err/int64(len(self.peerErrors))))) << 10
	} else {
		err = -1
	}
	return
}
func (self *Timer) Stability() (result int64) {
	if len(self.peerLatencies) > 1 {
		var deviation int64
		for _, latencies := range self.peerLatencies {
			_, deviation = latencies.stats()
			result += deviation * deviation
		}
		result = int64(math.Sqrt(float64(result / int64(len(self.peerLatencies)))))
	} else {
		result = -1
	}
	return
}
func (self *Timer) adjust(id string, adjustment int64) {
	self.peerErrors[id] = adjustment
	self.dilations.add(adjustment)
}
func (self *Timer) randomPeer() (id string, peer Peer, peerLatencies times) {
	currentPeers := self.peerProducer.Peers()
	chosenIndex := rand.Int() % len(currentPeers)
	for thisId, theseLatencies := range self.peerLatencies {
		if currentPeer, ok := currentPeers[thisId]; ok {
			if chosenIndex == 0 {
				peer = currentPeer
				id = thisId
				peerLatencies = theseLatencies
			}
			delete(currentPeers, thisId)
		} else {
			delete(self.peerLatencies, thisId)
			delete(self.peerErrors, thisId)
		}
		chosenIndex--
	}
	for thisId, thisPeer := range currentPeers {
		if chosenIndex == 0 {
			peer = thisPeer
			id = thisId
		}
		chosenIndex--
	}
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
	peerId, peer, oldLatencies := self.randomPeer()
	self.lock.RUnlock()

	peerTime, latency, myTime := self.timeAndLatency(peer)

	self.lock.Lock()
	defer self.lock.Unlock()
	oldestLatencyIndex := 0
	if len(oldLatencies) > loglen {
		oldestLatencyIndex = len(oldLatencies) - loglen
	}
	newLatencies := append(oldLatencies[oldestLatencyIndex:], latency)
	self.peerLatencies[peerId] = newLatencies

	mean, deviation := newLatencies.stats()
	if math.Abs(float64(latency-mean)) < float64(deviation) {
		self.adjust(peerId, peerTime-myTime)
	}
}
func (self *Timer) sleep() {
	err := self.Error()
	stability := self.Stability()
	if err == -1 || stability == -1 {
		time.Sleep(time.Second)
	} else {
		sleepyTime := ((time.Duration(stability) * time.Second) << 7) / time.Duration(err)
		if sleepyTime < time.Second {
			sleepyTime = time.Second
		}
		time.Sleep(sleepyTime)
	}
}
func (self *Timer) Run() {
	for {
		self.Sample()
		self.sleep()
	}
}
func (self *Timer) Start() {
	go self.Run()
}

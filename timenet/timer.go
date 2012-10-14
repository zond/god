package timenet

import (
	"math"
	"time"
)

const (
	loglen = 10
)

type Peer interface {
	GetTime() (time int64)
	GetName() (name string)
}

type PeerProducer interface {
	GetPeer() Peer
}

type latencies []int64

func (self latencies) stats() (mean, deviation int64) {
	var sum int64
	for _, latency := range self {
		sum += latency
	}
	mean = sum / int64(len(self))
	var squareSum int64
	var diff int64
	for _, latency := range self {
		diff = latency - mean
		squareSum += diff * diff
	}
	deviation = int64(math.Sqrt(float64(squareSum / (int64(len(self)) - 1))))
	return
}

type Timer struct {
	offset        int64
	peerProducer  PeerProducer
	peerLatencies map[string]latencies
}

func NewTimer(producer PeerProducer) *Timer {
	return &Timer{
		peerProducer:  producer,
		peerLatencies: make(map[string]latencies),
	}
}
func (self *Timer) Time() int64 {
	return time.Now().UnixNano() + self.offset
}
func (self *Timer) adjust(adjustment int64) {
	self.offset += adjustment
}
func (self *Timer) Sample() {
	peer := self.peerProducer.GetPeer()
	latency := -time.Now().UnixNano()
	peerTime := peer.GetTime()
	latency += time.Now().UnixNano()
	myTime := self.Time()
	peerTime += latency / 2

	oldLatencies := self.peerLatencies[peer.GetName()]
	oldestLatencyIndex := 0
	if len(oldLatencies) > loglen {
		oldestLatencyIndex = len(oldLatencies) - loglen
	}
	newLatencies := append(oldLatencies[oldestLatencyIndex:], latency)
	self.peerLatencies[peer.GetName()] = newLatencies

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

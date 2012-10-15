package timenet

import (
	"math"
	"time"
)

const (
	loglen         = 10
	dilationFactor = 10
)

type Peer interface {
	Time() (time int64)
	Name() (name string)
}

type PeerProducer interface {
	Peer() Peer
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
	if len(self) > 2 {
		deviation = int64(math.Sqrt(float64(squareSum / (int64(len(self)) - 1))))
	}
	return
}

type dilation struct {
	delta int64
	from  int64
}

func newDilation(delta int64) dilation {
	return dilation{delta, time.Now().UnixNano()}
}
func (self dilation) effect() (effect int64, done bool) {
	absDelta := self.delta
	if absDelta < 0 {
		absDelta *= -1
	}
	passed := float64(time.Now().UnixNano() - self.from)
	duration := float64(dilationFactor * absDelta)
	if passed > duration {
		effect = self.delta
		done = true
	} else {
		effect = int64(float64(self.delta) * (passed / duration))
		done = false
	}
	return
}

type dilations struct {
	content []dilation
}

func (self *dilations) delta() (sum int64) {
	for _, dilation := range self.content {
		sum += dilation.delta
	}
	return
}
func (self *dilations) effect() (temporaryEffect, permanentEffect int64) {
	newContent := make([]dilation, 0, len(self.content))
	for _, dilation := range self.content {
		thisEffect, done := dilation.effect()
		if done {
			permanentEffect += thisEffect
		} else {
			temporaryEffect += thisEffect
			newContent = append(newContent, dilation)
		}
	}
	self.content = newContent
	return
}
func (self *dilations) add(delta int64) {
	self.content = append(self.content, newDilation(delta))
}

type Timer struct {
	offset        int64
	dilations     *dilations
	peerProducer  PeerProducer
	peerLatencies map[string]latencies
}

func NewTimer(producer PeerProducer) *Timer {
	return &Timer{
		peerProducer:  producer,
		dilations:     &dilations{},
		peerLatencies: make(map[string]latencies),
	}
}
func (self *Timer) adjustments() int64 {
	return self.offset + self.dilations.delta()
}
func (self *Timer) ActualTime() int64 {
	return time.Now().UnixNano() + self.adjustments()
}
func (self *Timer) ContinuousTime() int64 {
	temporaryEffect, permanentEffect := self.dilations.effect()
	self.offset += permanentEffect
	return time.Now().UnixNano() + self.offset + temporaryEffect
}
func (self *Timer) adjust(adjustment int64) {
	self.dilations.add(adjustment)
}
func (self *Timer) Sample() {
	peer := self.peerProducer.Peer()
	latency := -time.Now().UnixNano()
	peerTime := peer.Time()
	latency += time.Now().UnixNano()
	myTime := self.ActualTime()
	peerTime += latency / 2

	oldLatencies := self.peerLatencies[peer.Name()]
	oldestLatencyIndex := 0
	if len(oldLatencies) > loglen {
		oldestLatencyIndex = len(oldLatencies) - loglen
	}
	newLatencies := append(oldLatencies[oldestLatencyIndex:], latency)
	self.peerLatencies[peer.Name()] = newLatencies

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

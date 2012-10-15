package timenet

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type testPeer struct {
	*Timer
}

func (self testPeer) ActualTime() (result int64) {
	time.Sleep((10 + time.Duration(rand.Int()%1)) * time.Millisecond)
	result = self.Timer.ActualTime()
	time.Sleep((10 + time.Duration(rand.Int()%1)) * time.Millisecond)
	return
}

type testPeerProducer struct {
	peers map[string]testPeer
}

func newTestPeerProducer() testPeerProducer {
	return testPeerProducer{make(map[string]testPeer)}
}

func (self testPeerProducer) makePeer() testPeer {
	timer := NewTimer(self)
	timer.offset = int64(rand.Int() % 1000000000)
	return testPeer{timer}
}
func (self testPeerProducer) deviance() (result int64) {
	var mean int64
	for _, timer := range self.peers {
		mean += timer.adjustments()
	}
	mean /= int64(len(self.peers))
	var delta int64
	for _, timer := range self.peers {
		delta = timer.adjustments() - mean
		result += delta * delta
	}
	return int64(math.Sqrt(float64(result / int64(len(self.peers)))))
}
func (self testPeerProducer) add(n string, p testPeer) {
	self.peers[n] = p
}
func (self testPeerProducer) Peers() (result map[string]Peer) {
	result = make(map[string]Peer)
	for n, p := range self.peers {
		result[n] = p
	}
	return
}

func TestSample(t *testing.T) {
	producer := newTestPeerProducer()
	peer1 := producer.makePeer()
	peer2 := producer.makePeer()
	peer3 := producer.makePeer()
	peer4 := producer.makePeer()
	producer.add("1", peer1)
	producer.add("2", peer2)
	producer.add("3", peer3)
	producer.add("4", peer4)
	for {
		fmt.Println("deviance:", producer.deviance())
		fmt.Println(time.Unix(0, peer1.Timer.ContinuousTime()))
		fmt.Println(time.Unix(0, peer2.Timer.ContinuousTime()))
		fmt.Println(time.Unix(0, peer3.Timer.ContinuousTime()))
		fmt.Println(time.Unix(0, peer4.Timer.ContinuousTime()))
		fmt.Println("sampling...")
		peer1.Sample()
		peer2.Sample()
		peer3.Sample()
		peer4.Sample()
		time.Sleep(time.Second)
	}
}

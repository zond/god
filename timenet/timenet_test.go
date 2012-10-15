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
	timer *Timer
	name  string
}

func (self testPeer) Time() (result int64) {
	time.Sleep(time.Duration(rand.Int()%5) * time.Millisecond)
	result = self.timer.ActualTime()
	time.Sleep(time.Duration(rand.Int()%5) * time.Millisecond)
	return
}
func (self testPeer) Name() string {
	return self.name
}
func (self testPeer) Sample() {
	self.timer.Sample()
}

type testPeerProducer struct {
	peers []testPeer
}

func (self *testPeerProducer) makePeer(name string) testPeer {
	timer := NewTimer(self)
	timer.offset = int64(rand.Int() % 100000000)
	return testPeer{timer, name}
}
func (self *testPeerProducer) deviance() (result int64) {
	var mean int64
	for _, timer := range self.peers {
		mean += timer.timer.adjustments()
	}
	mean /= int64(len(self.peers))
	var delta int64
	for _, timer := range self.peers {
		delta = timer.timer.adjustments() - mean
		result += delta * delta
	}
	return int64(math.Sqrt(float64(result / int64(len(self.peers)))))
}
func (self *testPeerProducer) add(p testPeer) {
	self.peers = append(self.peers, p)
}
func (self *testPeerProducer) Peer() Peer {
	return self.peers[rand.Int()%len(self.peers)]
}

func TestSample(t *testing.T) {
	producer := testPeerProducer{}
	peer1 := producer.makePeer("1")
	peer2 := producer.makePeer("2")
	peer3 := producer.makePeer("3")
	peer4 := producer.makePeer("4")
	producer.add(peer1)
	producer.add(peer2)
	producer.add(peer3)
	producer.add(peer4)
	for {
		fmt.Println("deviance:", producer.deviance())
		fmt.Println(time.Unix(0, peer1.timer.ContinuousTime()))
		fmt.Println(time.Unix(0, peer2.timer.ContinuousTime()))
		fmt.Println(time.Unix(0, peer3.timer.ContinuousTime()))
		fmt.Println(time.Unix(0, peer4.timer.ContinuousTime()))
		fmt.Println("sampling...")
		peer1.Sample()
		peer2.Sample()
		peer3.Sample()
		peer4.Sample()
		time.Sleep(time.Second)
	}
}

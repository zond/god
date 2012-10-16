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
	time.Sleep((10 + time.Duration(rand.Int()%1000)) * time.Microsecond)
	result = self.Timer.ActualTime()
	time.Sleep((10 + time.Duration(rand.Int()%1000)) * time.Microsecond)
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
	timer.offset = int64(rand.Int63() % int64(10000000000))
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
	var current1, current2, current3, current4 int64
	var last1, last2, last3, last4 int64
	for {
		fmt.Println("Offset standard deviance:", producer.deviance())
		current1 = peer1.Timer.ContinuousTime()
		fmt.Printf("%v\terr:%v\tstability:%v\n", time.Unix(0, current1), time.Duration(peer1.Error()), time.Duration(peer1.Stability()))
		if last1 != 0 && current1 < last1 {
			t.Fatalf("%v gave %v which is less than %v", peer1, current1, last1)
		}
		current2 = peer2.Timer.ContinuousTime()
		fmt.Printf("%v\terr:%v\tstability:%v\n", time.Unix(0, current2), time.Duration(peer2.Error()), time.Duration(peer2.Stability()))
		if last2 != 0 && current2 < last2 {
			t.Fatalf("%v gave %v which is less than %v", peer2, current2, last2)
		}
		current3 = peer3.Timer.ContinuousTime()
		fmt.Printf("%v\terr:%v\tstability:%v\n", time.Unix(0, current3), time.Duration(peer3.Error()), time.Duration(peer3.Stability()))
		if last3 != 0 && current3 < last3 {
			t.Fatalf("%v gave %v which is less than %v", peer3, current3, last3)
		}
		current4 = peer4.Timer.ContinuousTime()
		fmt.Printf("%v\terr:%v\tstability:%v\n", time.Unix(0, current4), time.Duration(peer4.Error()), time.Duration(peer4.Stability()))
		if last4 != 0 && current4 < last4 {
			t.Fatalf("%v gave %v which is less than %v", peer4, current4, last4)
		}
		fmt.Println("Sampling...")
		peer1.Sample()
		peer2.Sample()
		peer3.Sample()
		peer4.Sample()
		time.Sleep(time.Second)
	}
}

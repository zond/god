
package tree

import (
	"testing"
	"math/rand"
	"fmt"
	"crypto/sha1"
)

func TestTiger(t *testing.T) {
	tiger := &Tiger{}
	if string(tiger.Sum([]byte("martin"))) != string([]byte{158,5,123,89,250,107,181,252,220,8,139,139,226,90,4,210,121,173,234,27,112,213,124,50}) {
		t.Error("wrong checksum")
	}
}

func BenchmarkTiger(b *testing.B) {
	b.StopTimer()
	var v [][]byte
	t := &Tiger{}
	for i := 0; i < b.N; i++ {
		v = append(v, []byte(fmt.Sprint(rand.Int63())))
	}
	b.StartTimer()
	for _, n := range v {
		t.Sum(n)
	}
}

func BenchmarkSHA1(b *testing.B) {
	b.StopTimer()
	var v [][]byte
	t := sha1.New()
	for i := 0; i < b.N; i++ {
		v = append(v, []byte(fmt.Sprint(rand.Int63())))
	}
	b.StartTimer()
	for _, n := range v {
		t.Sum(n)
	}
}
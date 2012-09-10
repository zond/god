
package tree

import (
	"testing"
	"math/rand"
	"fmt"
	"crypto/sha1"
)

func TestTiger(t *testing.T) {
	tiger := &Tiger{}
	for i := 0; i < 1000; i++ {
		s := fmt.Sprint(rand.Int63())
		c1 := tiger.Sum([]byte(s))
		c2 := tiger.Sum([]byte(s))
		if string(c1) != string(c2) {
			t.Errorf("%v should == %v", c1, c2)
		}
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
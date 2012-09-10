
package murmur

import (
	"testing"
	"math/rand"
	"fmt"
	"crypto/sha1"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestMurmur(t *testing.T) {
	h := &Hash{}
	m := make(map[string]bool)
	for i := 0; i < 1000; i++ {
		s := fmt.Sprint(rand.Int63())
		c1 := h.Sum([]byte(s))
		c2 := h.Sum([]byte(s))
		if string(c1) != string(c2) {
			t.Errorf("%v should == %v", c1, c2)
		}
		if _, ok := m[s]; ok {
			t.Errorf("%v should not collide!", s)
		}
		m[s] = true
	}
}

func BenchmarkMurmur(b *testing.B) {
	b.StopTimer()
	var v [][]byte
	t := &Hash{}
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
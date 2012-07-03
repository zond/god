
package god

import (
	"testing"
	"fmt"
	"os"
)

func openGod(p string) *God {
	g, err := NewGod(p)
	if err == nil {
		return g
	}
	panic(fmt.Errorf("should be able to open %v, got %v", p, err))
}

func assertStored(t *testing.T, g *God, key, expected string) {
	if val, ok := g.Get(key); ok {
		if string(val) != expected {
			t.Errorf("expected to find %v under %v in %v, but got %v", expected, key, g, string(val))
		}
	} else {
		t.Errorf("couldn't find %v in %v, expected to find %v", key, g, expected)
	}
}

func BenchmarkPut(b *testing.B) {
	g := openGod("bench1")
	defer func() {
		os.Remove("bench1")
	}()
	data := make([]byte, 128)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		g.Put(fmt.Sprint(i), data)
	}
	b.StopTimer()
}

func TestBlaj(t *testing.T) {
	g := openGod("test1")
	g.Put("k", []byte("v"))
	assertStored(t, g, "k", "v")
}
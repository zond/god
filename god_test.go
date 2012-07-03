
package god

import (
	"testing"
)

func openGod(t *testing.T, p string) *God {
	if g, err := NewGod(p); err == nil {
		return g
	} else {
		t.Errorf("should be able to open %v, got %v", p, err)
	}
	return nil
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

func TestBlaj(t *testing.T) {
	g := openGod(t, "test1")
	g.Put("k", []byte("v"))
	assertStored(t, g, "k", "v")
}

package tree

import (
	"testing"
	"fmt"
	"reflect"
)

func assertMappness(t *testing.T, tree *Tree, m map[string]Thing) {
	if !reflect.DeepEqual(tree.ToMap(), m) {
		t.Errorf("%v should be %v", tree, m)
	}
	if tree.Size() != len(m) {
		t.Errorf("%v.Size() should be %v", tree, len(m))
	}
}

func TestTreeEach(t *testing.T) {
	tree := new(Tree)
	m := make(map[string]Thing)
	for i := 0; i < 10; i++ {
		tree.Put([]byte(fmt.Sprint(i)), i)
		if val, exists := tree.Get([]byte(fmt.Sprint(i))); val != i || !exists {
			t.Errorf("insert of %v failed!", i)
		}
		m[string([]byte(fmt.Sprint(i)))] = i
	}
	assertMappness(t, tree, m)
	var collector []string
	tree.Each(Up, []byte("5"), []byte("7"), func(key []byte, value Thing) {
		collector = append(collector, string(key))
	})
	if !reflect.DeepEqual(collector, []string{"5", "6", "7"}) {
		t.Errorf("%v is bad", collector)
	}
	collector = nil
	tree.Each(Down, []byte("4"), []byte("6"), func(key []byte, value Thing) {
		collector = append(collector, string(key))
	})
	if !reflect.DeepEqual(collector, []string{"6", "5", "4"}) {
		t.Errorf("%v is bad", collector)
	}
}

func TestTreeBasicOps(t *testing.T) {
	tree := new(Tree)
	m := make(map[string]Thing)
	assertMappness(t, tree, m)
	if val, existed := tree.Get([]byte("key")); val != nil || existed {
		t.Errorf("should not have existed")
	}
	if old, existed := tree.Del([]byte("key")); old != nil || existed {
		t.Errorf("should not have existed")
	}
	if old, existed := tree.Put([]byte("key"), "value"); old != nil || existed {
		t.Errorf("should not have existed")
	}
	if val, existed := tree.Get([]byte("key")); val != "value" || !existed {
		t.Errorf("should not have existed")
	}
	m["key"] = "value"
	assertMappness(t, tree, m)
	if old, existed := tree.Put([]byte("key"), "value2"); old != "value" || !existed {
		t.Errorf("should have existed")
	}
	if val, existed := tree.Get([]byte("key")); val != "value2" || !existed {
		t.Errorf("should have existed")
	}
	m["key"] = "value2"
	assertMappness(t, tree, m)
	if old, existed := tree.Del([]byte("key")); old != "value2" || !existed {
		t.Errorf("should have existed")
	}
	delete(m, "key")
	assertMappness(t, tree, m)
	if old, existed := tree.Del([]byte("key")); old != nil || existed {
		t.Errorf("should not have existed")
	}
}

func benchTree(b *testing.B, n int) {
	b.StopTimer()
	var v [][]byte
	for i := 0; i < n; i++ {
		v = append(v, []byte(fmt.Sprint(i)))
	}
	b.StartTimer()
	for j := 0; j < b.N / n; j++ {
		m := new(Tree)
		for i := 0; i < n; i++ {
			k := v[i]
			m.Put(k, i)
			j, _ := m.Get(k)
			if j != i {
				b.Error("should be same value")
			}
		}
	}
}

func BenchmarkTree10(b *testing.B) {
	benchTree(b, 10)
}
func BenchmarkTree100(b *testing.B) {
	benchTree(b, 100)
}
func BenchmarkTree1000(b *testing.B) {
	benchTree(b, 1000)
}
func BenchmarkTree10000(b *testing.B) {
	benchTree(b, 10000)
}
func BenchmarkTree100000(b *testing.B) {
	benchTree(b, 100000)
}
func BenchmarkTree1000000(b *testing.B) {
	benchTree(b, 1000000)
}


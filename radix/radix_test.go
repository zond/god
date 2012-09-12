
package radix

import (
	"fmt"
	"testing"
)

func assertExistance(t *testing.T, tree *Tree, k, v string) {
	if value, existed := tree.Get([]byte(k)); !existed || value != StringHasher(v) {
		t.Errorf("%v should contain %v => %v, got %v, %v", tree, k, v, value, existed)
	}
}

func TestRadixBasicOps(t *testing.T) {
	tree := NewTree()
	tree.Put([]byte("apple"), StringHasher("fruit"))
	tree.Put([]byte("crab"), StringHasher("animal"))
	tree.Put([]byte("crabapple"), StringHasher("fruit"))
	tree.Put([]byte("banana"), StringHasher("fruit"))
	tree.Put([]byte("guava"), StringHasher("fruit"))
	tree.Put([]byte("guanabana"), StringHasher("city"))
	assertExistance(t, tree, "apple", "fruit")
	assertExistance(t, tree, "crab", "animal")
	assertExistance(t, tree, "crabapple", "fruit")
	assertExistance(t, tree, "banana", "fruit")
	assertExistance(t, tree, "guava", "fruit")
	assertExistance(t, tree, "guanabana", "city")
}

func benchTree(b *testing.B, n int) {
	b.StopTimer()
	var k [][]byte
	var v []Hasher
	for i := 0; i < n; i++ {
		k = append(k, []byte(fmt.Sprint(i)))
		v = append(v, StringHasher(fmt.Sprint(i)))
	}
	b.StartTimer()
	for j := 0; j < b.N/n; j++ {
		m := NewTree()
		for i := 0; i < n; i++ {
			m.Put(k[i], v[i])
			j, existed := m.Get(k[i])
			if j != v[i] {
				b.Fatalf("%v should contain %v, but got %v, %v", m.Describe(), string(k[i]), j, existed)
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


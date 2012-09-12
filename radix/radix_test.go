
package radix

import (
	"../murmur"
	"fmt"
	"testing"
	"math/rand"
	"bytes"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func assertExistance(t *testing.T, tree *Tree, k, v string) {
	if value, existed := tree.Get([]byte(k)); !existed || value != StringHasher(v) {
		t.Errorf("%v should contain %v => %v, got %v, %v", tree, k, v, value, existed)
	}
}

func assertNonExistance(t *testing.T, tree *Tree, k string) {
	if value, existed := tree.Get([]byte(k)); existed || value != nil {
		t.Errorf("%v should not contain %v, got %v, %v", tree, k, value, existed)
	}
}

func TestRadixHash(t *testing.T) {
	tree1 := NewTree()
	var keys [][]byte
	var vals []StringHasher
	n := 10000
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprint(rand.Int63()))
		v := StringHasher(fmt.Sprint(rand.Int63()))
		keys = append(keys, k)
		vals = append(vals, v)
		tree1.Put(k, v)
	}
	tree2 := NewTree()
	for i := 0; i < n; i++ {
		index := rand.Int() % len(keys)
		k := keys[index]
		v := vals[index]
		tree2.Put(k, v)
		keys = append(keys[:index], keys[index + 1:]...)
		vals = append(vals[:index], vals[index + 1:]...)
	}
	if bytes.Compare(tree1.Hash(), tree2.Hash()) != 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should be equal!", tree1.Describe(), tree2.Describe(), tree1.Hash(), tree2.Hash())
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
	tree.Put([]byte("crab"), StringHasher("crustacean"))
	assertExistance(t, tree, "crab", "crustacean")
	assertNonExistance(t, tree, "gua")
}

func TestRadixNilKeyPut(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Error("should raise exception!")
		}
	}()
	tree := NewTree()
	tree.Put(nil, StringHasher("nil"))
	t.Error("should raise exception!")
}

func TestRadixNilKeyGet(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Error("should raise exception!")
		}
	}()
	tree := NewTree()
	tree.Get(nil)
	t.Error("should raise exception!")
}

func benchTree(b *testing.B, n int) {
	b.StopTimer()
	var k [][]byte
	var v []Hasher
	for i := 0; i < n; i++ {
		k = append(k, murmur.HashString(fmt.Sprint(i)))
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


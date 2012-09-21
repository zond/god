package radix

import (
	"../murmur"
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"
	"reflect"
)

var benchmarkTestTree *Tree
var benchmarkTestKeys [][]byte
var benchmarkTestValues []Hasher

func init() {
	rand.Seed(time.Now().UnixNano())
	benchmarkTestTree = NewTree()
}


func TestRadixSyncComplete(t *testing.T) {
	tree1 := NewTree()
	n := 1000
	var k []byte
	var v StringHasher
	for i := 0; i < n; i++ {
		k = []byte(fmt.Sprint(i))
		v = StringHasher(fmt.Sprint(i))
		tree1.Put(k, v)
	}
	tree2 := NewTree()
	s := NewSync(tree1, tree2)
	s.Run()
	if bytes.Compare(tree1.Hash(), tree2.Hash()) != 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should be equal!", tree1.Describe(), tree2.Describe(), tree1.Hash(), tree2.Hash())
	}
	if !reflect.DeepEqual(tree1, tree2) {
		t.Errorf("%v and %v are unequal", tree1, tree2)
	}
}

func TestRadixSyncPartial(t *testing.T) {
	tree1 := NewTree()
	tree2 := NewTree()
	n := 1000
	var k []byte
	var v StringHasher
	for i := 0; i < n; i++ {
		k = []byte(fmt.Sprint(i))
		v = StringHasher(fmt.Sprint(i))
		tree1.Put(k, v)
		if n % 10 != 0 {
			tree2.Put(k, v)
		}
	}
	s := NewSync(tree1, tree2)
	s.Run()
	if bytes.Compare(tree1.Hash(), tree2.Hash()) != 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should be equal!", tree1.Describe(), tree2.Describe(), tree1.Hash(), tree2.Hash())
	}
	if !reflect.DeepEqual(tree1, tree2) {
		t.Errorf("%v and %v are unequal", tree1, tree2)
	}
}

func TestRadixHash(t *testing.T) {
	tree1 := NewTree()
	var keys [][]byte
	var vals []StringHasher
	n := 10
	var k []byte
	var v StringHasher
	for i := 0; i < n; i++ {
		k = []byte(fmt.Sprint(rand.Int63()))
		v = StringHasher(fmt.Sprint(rand.Int63()))
		keys = append(keys, k)
		vals = append(vals, v)
		tree1.Put(k, v)
	}
	keybackup := keys
	tree2 := NewTree()
	for i := 0; i < n; i++ {
		index := rand.Int() % len(keys)
		k = keys[index]
		v = vals[index]
		tree2.Put(k, v)
		keys = append(keys[:index], keys[index+1:]...)
		vals = append(vals[:index], vals[index+1:]...)
	}
	if bytes.Compare(tree1.Hash(), tree2.Hash()) != 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should be equal!", tree1.Describe(), tree2.Describe(), tree1.Hash(), tree2.Hash())
	}
	if !reflect.DeepEqual(tree1.Finger(nil), tree2.Finger(nil)) {
		t.Errorf("%v and %v have prints\n%v\n%v\nand they should be equal!", tree1.Describe(), tree2.Describe(), tree1.Finger(nil), tree2.Finger(nil))
	}
	tree1.Each(func(key []byte, value Hasher) {
		f1 := tree1.Finger(rip(key))
		f2 := tree2.Finger(rip(key))
		if f1 == nil || f2 == nil {
			t.Errorf("should not be nil!")
		}
		if !reflect.DeepEqual(f1, f2) {
			t.Errorf("should be equal!")
		}
	})
	var deletes []int
	for i := 0; i < n/10; i++ {
		index := rand.Int() % len(keybackup)
		deletes = append(deletes, index)
	}
	var successes []bool
	for i := 0; i < n/10; i++ {
		_, ok := tree1.Del(keybackup[deletes[i]])
		successes = append(successes, ok)
	}
	for i := 0; i < n/10; i++ {
		if _, ok := tree2.Del(keybackup[deletes[i]]); ok != successes[i] {
			t.Errorf("delete success should be %v", successes[i])
		}
	}
	if bytes.Compare(tree1.Hash(), tree2.Hash()) != 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should be equal!", tree1.Describe(), tree2.Describe(), tree1.Hash(), tree2.Hash())
	}
	if !reflect.DeepEqual(tree1.Finger(nil), tree2.Finger(nil)) {
		t.Errorf("%v and %v have prints\n%v\n%v\nand they should be equal!", tree1.Describe(), tree2.Describe(), tree1.Finger(nil), tree2.Finger(nil))
	}
	tree1.Each(func(key []byte, value Hasher) {
		f1 := tree1.Finger(rip(key))
		f2 := tree2.Finger(rip(key))
		if f1 == nil || f2 == nil {
			t.Errorf("should not be nil!")
		}
		if !reflect.DeepEqual(f1, f2) {
			t.Errorf("should be equal!")
		}
	})
}

func TestRadixNilKey(t *testing.T) {
	tree := NewTree()
	h := tree.Hash()
	if value, existed := tree.Get(nil); value != nil || existed {
		t.Errorf("should not exist")
	}
	if value, existed := tree.Put(nil, nil); value != nil || existed {
		t.Errorf("should not exist")
	}
	if value, existed := tree.Get(nil); value != nil || !existed {
		t.Errorf("should exist")
	}
	if value, existed := tree.Del(nil); value != nil || !existed {
		t.Errorf("should exist")
	}
	if value, existed := tree.Get(nil); value != nil || existed {
		t.Errorf("should not exist")
	}
	if bytes.Compare(h, tree.Hash()) != 0 {
		t.Errorf("should be equal")
	}
}

func TestRadixBasicOps(t *testing.T) {
	tree := NewTree()
	assertNewPut(t, tree, "apple", "stonefruit")
	assertOldPut(t, tree, "apple", "fruit", "stonefruit")
	assertNewPut(t, tree, "crab", "critter")
	assertOldPut(t, tree, "crab", "animal", "critter")
	assertNewPut(t, tree, "crabapple", "poop")
	assertOldPut(t, tree, "crabapple", "fruit", "poop")
	assertNewPut(t, tree, "banana", "yellow")
	assertOldPut(t, tree, "banana", "fruit", "yellow")
	assertNewPut(t, tree, "guava", "fart")
	assertOldPut(t, tree, "guava", "fruit", "fart")
	assertNewPut(t, tree, "guanabana", "man")
	assertOldPut(t, tree, "guanabana", "city", "man")
	m := make(map[string]Hasher)
	tree.Each(func(key []byte, value Hasher) {
		m[string(key)] = value
	})
	comp := map[string]Hasher{
		"apple": StringHasher("fruit"),
		"crab": StringHasher("animal"),
		"crabapple": StringHasher("fruit"),
		"banana": StringHasher("fruit"),
		"guava": StringHasher("fruit"),
		"guanabana": StringHasher("city"),
	}
	if !reflect.DeepEqual(m, comp) {
		t.Errorf("should be equal!")
	}
	if !reflect.DeepEqual(tree.ToMap(), comp) {
		t.Errorf("should be equal!")
	}
	if old, existed := tree.Put(nil, StringHasher("nil")); old != nil || existed {
		t.Error("should not exist yet")
	}
	if old, existed := tree.Put([]byte("nil"), nil); old != nil || existed {
		t.Error("should not exist yet")
	}
	if value, existed := tree.Get(nil); !existed || value != StringHasher("nil") {
		t.Errorf("%v should contain %v => %v, got %v, %v", tree, nil, "nil", value, existed)
	}
	if value, existed := tree.Get([]byte("nil")); !existed || value != nil {
		t.Errorf("%v should contain %v => %v, got %v, %v", tree, "nil", nil, value, existed)
	}
	assertDelFailure(t, tree, "gua")
	assertDelSuccess(t, tree, "apple", "fruit")
	assertDelFailure(t, tree, "apple")
	assertDelSuccess(t, tree, "crab", "animal")
	assertDelFailure(t, tree, "crab")
	assertDelSuccess(t, tree, "crabapple", "fruit")
	assertDelFailure(t, tree, "crabapple")
	assertDelSuccess(t, tree, "banana", "fruit")
	assertDelFailure(t, tree, "banana")
	assertDelSuccess(t, tree, "guava", "fruit")
	assertDelFailure(t, tree, "guava")
	assertDelSuccess(t, tree, "guanabana", "city")
	assertDelFailure(t, tree, "guanabana")
}

func benchTreeSync(b *testing.B, size, diff int) {
	b.StopTimer()
	tree1 := NewTree()
	tree2 := NewTree()
	mod := size / diff
	var k []byte
	var v Hasher
	for i := 0; i < size; i++ {
		k = murmur.HashString(fmt.Sprint(i))
		v = StringHasher(fmt.Sprint(i))
		tree1.Put(k, v)
		if size % mod != 0 {
			tree2.Put(k, v)
		}
	}
	b.StartTimer()
	s := NewSync(tree1, tree2)
	s.Run()
	b.StopTimer()
	if bytes.Compare(tree1.Hash(), tree2.Hash()) != 0 {
		b.Errorf("%v and %v have hashes\n%v\n%v\nand they should be equal!", tree1.Describe(), tree2.Describe(), tree1.Hash(), tree2.Hash())
	}
	if !reflect.DeepEqual(tree1, tree2) {
		b.Errorf("%v and %v are unequal", tree1, tree2)
	}
}

func BenchmarkTreeSync100_1(b *testing.B) {
	benchTreeSync(b, 100, 1)
}

func benchTree(b *testing.B, n int) {
	b.StopTimer()
	for len(benchmarkTestKeys) < n {
		benchmarkTestKeys = append(benchmarkTestKeys, murmur.HashString(fmt.Sprint(len(benchmarkTestKeys))))
		benchmarkTestValues = append(benchmarkTestValues, StringHasher(fmt.Sprint(len(benchmarkTestValues))))
	}
	for benchmarkTestTree.Size() < n {
		benchmarkTestTree.Put(benchmarkTestKeys[benchmarkTestTree.Size()], benchmarkTestValues[benchmarkTestTree.Size()])
	}
	var keys [][]byte
	var vals []Hasher
	for i := 0; i < b.N; i++ {
		keys = append(keys, murmur.HashString(fmt.Sprint(rand.Int63())))
		vals = append(vals, StringHasher(fmt.Sprint(rand.Int63())))
	}
	var k []byte
	var v Hasher
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		k = benchmarkTestKeys[i%len(benchmarkTestKeys)]
		v = benchmarkTestValues[i%len(benchmarkTestValues)]
		benchmarkTestTree.Put(k, v)
		j, existed := benchmarkTestTree.Get(k)
		if j != v {
			b.Fatalf("%v should contain %v, but got %v, %v", benchmarkTestTree.Describe(), v, j, existed)
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

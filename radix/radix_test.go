package radix

import (
	"../common"
	"../murmur"
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

var benchmarkTestTree *Tree
var benchmarkTestKeys [][]byte
var benchmarkTestValues []Hasher

func init() {
	rand.Seed(time.Now().UnixNano())
	benchmarkTestTree = NewTree()
}

func TestSyncVersions(t *testing.T) {
	tree1 := NewTree()
	tree3 := NewTree()
	n := 10
	var k []byte
	var v StringHasher
	for i := 0; i < n; i++ {
		k = []byte{byte(i)}
		v = StringHasher(fmt.Sprint(i))
		tree1.Put(k, v, 0)
		if i == 2 {
			tree3.Put(k, StringHasher("other version"), 2)
		} else {
			tree3.Put(k, v, 0)
		}
	}
	tree2 := NewTree()
	tree2.Put([]byte{2}, StringHasher("other version"), 2)
	s := NewSync(tree1, tree2)
	s.Run()
	if bytes.Compare(tree1.Hash(), tree2.Hash()) == 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should not be equal!", tree1.Describe(), tree2.Describe(), tree1.Hash(), tree2.Hash())
	}
	if reflect.DeepEqual(tree1, tree2) {
		t.Errorf("%v and %v are equal", tree1, tree2)
	}
	if bytes.Compare(tree3.Hash(), tree2.Hash()) != 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should be equal!", tree3.Describe(), tree2.Describe(), tree3.Hash(), tree2.Hash())
	}
	if !reflect.DeepEqual(tree3, tree2) {
		t.Errorf("%v and %v are unequal", tree3, tree2)
	}
	tree1.Put([]byte{2}, StringHasher("yet another version"), 3)
	s.Run()
	if bytes.Compare(tree3.Hash(), tree2.Hash()) == 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should not be equal!", tree3.Describe(), tree2.Describe(), tree3.Hash(), tree2.Hash())
	}
	if reflect.DeepEqual(tree3, tree2) {
		t.Errorf("%v and %v are equal", tree3, tree2)
	}
	if bytes.Compare(tree1.Hash(), tree2.Hash()) != 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should be equal!", tree1.Describe(), tree2.Describe(), tree1.Hash(), tree2.Hash())
	}
	if !reflect.DeepEqual(tree1, tree2) {
		t.Errorf("%v and %v are unequal", tree1, tree2)
	}
}

func TestSyncLimits(t *testing.T) {
	tree1 := NewTree()
	tree3 := NewTree()
	n := 10
	from := 3
	to := 7
	fromKey := []byte{byte(from)}
	toKey := []byte{byte(to)}
	var k []byte
	var v StringHasher
	for i := 0; i < n; i++ {
		k = []byte{byte(i)}
		v = StringHasher(fmt.Sprint(i))
		tree1.Put(k, v, 0)
		if i >= from && i < to {
			tree3.Put(k, v, 0)
		}
	}
	tree2 := NewTree()
	s := NewSync(tree1, tree2).From(fromKey).To(toKey)
	s.Run()
	if bytes.Compare(tree3.Hash(), tree2.Hash()) != 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should be equal!", tree3.Describe(), tree2.Describe(), tree3.Hash(), tree2.Hash())
	}
	if !reflect.DeepEqual(tree3, tree2) {
		t.Errorf("%v and %v are unequal", tree3, tree2)
	}
}

func TestRipStitch(t *testing.T) {
	var b []byte
	for i := 0; i < 1000; i++ {
		b = make([]byte, rand.Int()%30)
		for j := 0; j < len(b); j++ {
			b[j] = byte(rand.Int())
		}
		if bytes.Compare(stitch(rip(b)), b) != 0 {
			t.Errorf("%v != %v", stitch(rip(b)), b)
		}
	}
}

func TestSyncSubTreeDestructive(t *testing.T) {
	tree1 := NewTree()
	tree3 := NewTree()
	n := 10
	var k, sk []byte
	var v StringHasher
	for i := 0; i < n; i++ {
		k = []byte(murmur.HashString(fmt.Sprint(i)))
		v = StringHasher(fmt.Sprint(i))
		if i%2 == 0 {
			tree1.Put(k, v, 0)
			tree3.Put(k, v, 0)
		} else {
			for j := 0; j < 10; j++ {
				sk = []byte(fmt.Sprint(j))
				tree1.SubPut(k, sk, v, 0)
				tree3.SubPut(k, sk, v, 0)
			}
		}
	}
	tree2 := NewTree()
	s := NewSync(tree3, tree2).Destroy()
	s.Run()
	if bytes.Compare(tree1.Hash(), tree2.Hash()) != 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should be equal!", tree1.Describe(), tree2.Describe(), tree1.Hash(), tree2.Hash())
	}
	if !reflect.DeepEqual(tree1, tree2) {
		t.Errorf("\n%v and \n%v are unequal", tree1.Describe(), tree2.Describe())
	}
	if tree3.Size() != 0 {
		t.Errorf("should be empty")
	}
	if !reflect.DeepEqual(tree3, NewTree()) {
		t.Errorf("%v and %v should be equal", tree3.Describe(), NewTree().Describe())
	}
}

func TestSyncSubTreeVersions(t *testing.T) {
	tree1 := NewTree()
	tree3 := NewTree()
	n := 10
	var k, sk []byte
	var v StringHasher
	for i := 0; i < n; i++ {
		k = []byte(murmur.HashString(fmt.Sprint(i)))
		v = StringHasher(fmt.Sprint(i))
		if i%2 == 0 {
			tree3.Put(k, v, 0)
			tree1.Put(k, v, 0)
		} else {
			for j := 0; j < 10; j++ {
				sk = []byte(fmt.Sprint(j))
				tree1.SubPut(k, sk, v, 0)
				if i == 1 && j == 3 {
					tree3.SubPut(k, sk, StringHasher("another value"), 2)
				} else {
					tree3.SubPut(k, sk, v, 0)
				}
			}
		}
	}
	tree2 := NewTree()
	tree2.SubPut([]byte(murmur.HashString(fmt.Sprint(1))), []byte(fmt.Sprint(3)), StringHasher("another value"), 2)
	s := NewSync(tree1, tree2)
	s.Run()
	if bytes.Compare(tree1.Hash(), tree2.Hash()) == 0 {
		t.Errorf("should not be equal")
	}
	if reflect.DeepEqual(tree1, tree2) {
		t.Errorf("should not be equal")
	}
	if bytes.Compare(tree3.Hash(), tree2.Hash()) != 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should be equal!", tree3.Describe(), tree2.Describe(), tree3.Hash(), tree2.Hash())
	}
	if !reflect.DeepEqual(tree3, tree2) {
		t.Errorf("\n%v and \n%v are unequal", tree3.Describe(), tree2.Describe())
	}
	tree1.SubPut([]byte(murmur.HashString(fmt.Sprint(1))), []byte(fmt.Sprint(3)), StringHasher("another value again"), 3)
	s.Run()
	if bytes.Compare(tree3.Hash(), tree2.Hash()) == 0 {
		t.Errorf("should not be equal")
	}
	if reflect.DeepEqual(tree3, tree2) {
		t.Errorf("should not be equal")
	}
	if bytes.Compare(tree1.Hash(), tree2.Hash()) != 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should be equal!", tree1.Describe(), tree2.Describe(), tree1.Hash(), tree2.Hash())
	}
	if !reflect.DeepEqual(tree1, tree2) {
		t.Errorf("\n%v and \n%v are unequal", tree1.Describe(), tree2.Describe())
	}
}
func TestSyncSubTree(t *testing.T) {
	tree1 := NewTree()
	n := 10
	var k, sk []byte
	var v StringHasher
	for i := 0; i < n; i++ {
		k = []byte(murmur.HashString(fmt.Sprint(i)))
		v = StringHasher(fmt.Sprint(i))
		if i%2 == 0 {
			tree1.Put(k, v, 0)
		} else {
			for j := 0; j < 10; j++ {
				sk = []byte(fmt.Sprint(j))
				tree1.SubPut(k, sk, v, 0)
			}
		}
	}
	tree2 := NewTree()
	s := NewSync(tree1, tree2)
	s.Run()
	if bytes.Compare(tree1.Hash(), tree2.Hash()) != 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should be equal!", tree1.Describe(), tree2.Describe(), tree1.Hash(), tree2.Hash())
	}
	if !reflect.DeepEqual(tree1, tree2) {
		t.Errorf("\n%v and \n%v are unequal", tree1.Describe(), tree2.Describe())
	}
}

func TestSyncDestructive(t *testing.T) {
	tree1 := NewTree()
	tree3 := NewTree()
	n := 1000
	var k []byte
	var v StringHasher
	for i := 0; i < n; i++ {
		k = []byte(fmt.Sprint(i))
		v = StringHasher(fmt.Sprint(i))
		tree1.Put(k, v, 0)
		tree3.Put(k, v, 0)
	}
	tree2 := NewTree()
	s := NewSync(tree3, tree2).Destroy()
	s.Run()
	if bytes.Compare(tree1.Hash(), tree2.Hash()) != 0 {
		t.Errorf("%v and %v have hashes\n%v\n%v\nand they should be equal!", tree1.Describe(), tree2.Describe(), tree1.Hash(), tree2.Hash())
	}
	if !reflect.DeepEqual(tree1, tree2) {
		t.Errorf("%v and %v are unequal", tree1, tree2)
	}
	if tree3.Size() != 0 {
		t.Errorf("%v should be size 0, is size %v", tree3, tree3.Size())
	}
	if !reflect.DeepEqual(tree3, NewTree()) {
		t.Errorf("should be equal")
	}
}

func TestSyncDestructiveMatching(t *testing.T) {
	tree1 := NewTree()
	tree2 := NewTree()
	tree3 := NewTree()
	n := 1000
	var k []byte
	var v StringHasher
	for i := 0; i < n; i++ {
		k = murmur.HashString(fmt.Sprint(i))
		v = StringHasher(fmt.Sprint(i))
		tree1.Put(k, v, 0)
		tree2.Put(k, v, 0)
		tree3.Put(k, v, 0)
	}
	NewSync(tree1, tree2).Destroy().Run()
	if !reflect.DeepEqual(tree2, tree3) {
		t.Errorf("should be equal")
	}
	if tree1.Size() != 0 {
		t.Errorf("should be empty!")
	}
	tree4 := NewTree()
	if !reflect.DeepEqual(tree1, tree4) {
		t.Errorf("should be equal!")
	}
}

func TestSyncComplete(t *testing.T) {
	tree1 := NewTree()
	n := 1000
	var k []byte
	var v StringHasher
	for i := 0; i < n; i++ {
		k = []byte(fmt.Sprint(i))
		v = StringHasher(fmt.Sprint(i))
		tree1.Put(k, v, 0)
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

func TestSyncRandomLimits(t *testing.T) {
	tree1 := NewTree()
	n := 10
	var k []byte
	var v StringHasher
	for i := 0; i < n; i++ {
		k = murmur.HashString(fmt.Sprint(i))
		v = StringHasher(fmt.Sprint(i))
		tree1.Put(k, v, 0)
	}
	var keys [][]byte
	tree1.Each(func(key []byte, value Hasher) {
		keys = append(keys, key)
	})
	var fromKey []byte
	var toKey []byte
	var tree2 *Tree
	var tree3 *Tree
	var s *Sync
	for fromIndex, _ := range keys {
		for toIndex, _ := range keys {
			if fromIndex != toIndex {
				fromKey = keys[fromIndex]
				toKey = keys[toIndex]
				tree2 = NewTree()
				tree1.Each(func(key []byte, value Hasher) {
					if common.BetweenIE(key, fromKey, toKey) {
						tree2.Put(key, value, 0)
					}
				})
				tree3 = NewTree()
				s = NewSync(tree1, tree3).From(fromKey).To(toKey)
				s.Run()
				if !reflect.DeepEqual(tree3, tree2) {
					t.Errorf("when syncing from %v to %v, %v and %v have hashes\n%v\n%v\nand they should be equal!", common.HexEncode(fromKey), common.HexEncode(toKey), tree3.Describe(), tree2.Describe(), tree3.Hash(), tree2.Hash())
				}
			}
		}
	}
}

func TestSyncPartial(t *testing.T) {
	tree1 := NewTree()
	tree2 := NewTree()
	mod := 2
	n := 100
	var k []byte
	var v StringHasher
	for i := 0; i < n; i++ {
		k = []byte(fmt.Sprint(i))
		v = StringHasher(fmt.Sprint(i))
		tree1.Put(k, v, 0)
		if i%mod != 0 {
			tree2.Put(k, v, 0)
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

func TestTreeHash(t *testing.T) {
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
		tree1.Put(k, v, 0)
	}
	keybackup := keys
	tree2 := NewTree()
	for i := 0; i < n; i++ {
		index := rand.Int() % len(keys)
		k = keys[index]
		v = vals[index]
		tree2.Put(k, v, 0)
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

func TestTreeNilKey(t *testing.T) {
	tree := NewTree()
	h := tree.Hash()
	if value, _, existed := tree.Get(nil); value != nil || existed {
		t.Errorf("should not exist")
	}
	if value, existed := tree.Put(nil, nil, 0); value != nil || existed {
		t.Errorf("should not exist")
	}
	if value, _, existed := tree.Get(nil); value != nil || !existed {
		t.Errorf("should exist")
	}
	if value, existed := tree.Del(nil); value != nil || !existed {
		t.Errorf("should exist")
	}
	if value, _, existed := tree.Get(nil); value != nil || existed {
		t.Errorf("should not exist")
	}
	if bytes.Compare(h, tree.Hash()) != 0 {
		t.Errorf("should be equal")
	}
}

func TestTreePrev(t *testing.T) {
	tree := NewTree()
	for i := 100; i < 200; i++ {
		tree.Put([]byte(fmt.Sprint(i)), StringHasher(fmt.Sprint(i)), 0)
	}
	for i := 101; i < 200; i++ {
		if key, value, _, existed := tree.Prev([]byte(fmt.Sprint(i))); string(key) != fmt.Sprint(i-1) || value != StringHasher(fmt.Sprint(i-1)) || !existed {
			t.Errorf("%v, %v, %v should be %v, %v, %v", string(key), value, existed, fmt.Sprint(i-1), StringHasher(fmt.Sprint(i-1)), true)
		}
	}
	if key, value, _, existed := tree.Prev([]byte("100")); existed {
		t.Errorf("%v, %v, %v should not exist!", key, value, existed)
	}
}

func TestTreeNext(t *testing.T) {
	tree := NewTree()
	for i := 100; i < 200; i++ {
		tree.Put([]byte(fmt.Sprint(i)), StringHasher(fmt.Sprint(i)), 0)
	}
	for i := 100; i < 199; i++ {
		if key, value, _, existed := tree.Next([]byte(fmt.Sprint(i))); string(key) != fmt.Sprint(i+1) || value != StringHasher(fmt.Sprint(i+1)) || !existed {
			t.Errorf("%v, %v, %v should be %v, %v, %v", string(key), value, existed, fmt.Sprint(i+1), StringHasher(fmt.Sprint(i+1)), true)
		}
	}
	if key, value, _, existed := tree.Next([]byte("199")); existed {
		t.Errorf("%v, %v, %v should not exist!", key, value, existed)
	}
}

func TestTreeBasicOps(t *testing.T) {
	tree := NewTree()
	assertSize(t, tree, 0)
	assertNewPut(t, tree, "apple", "stonefruit")
	assertSize(t, tree, 1)
	assertOldPut(t, tree, "apple", "fruit", "stonefruit")
	assertSize(t, tree, 1)
	assertNewPut(t, tree, "crab", "critter")
	assertSize(t, tree, 2)
	assertOldPut(t, tree, "crab", "animal", "critter")
	assertSize(t, tree, 2)
	assertNewPut(t, tree, "crabapple", "poop")
	assertSize(t, tree, 3)
	assertOldPut(t, tree, "crabapple", "fruit", "poop")
	assertSize(t, tree, 3)
	assertNewPut(t, tree, "banana", "yellow")
	assertSize(t, tree, 4)
	assertOldPut(t, tree, "banana", "fruit", "yellow")
	assertSize(t, tree, 4)
	assertNewPut(t, tree, "guava", "fart")
	assertSize(t, tree, 5)
	assertOldPut(t, tree, "guava", "fruit", "fart")
	assertSize(t, tree, 5)
	assertNewPut(t, tree, "guanabana", "man")
	assertSize(t, tree, 6)
	assertOldPut(t, tree, "guanabana", "city", "man")
	assertSize(t, tree, 6)
	m := make(map[string]Hasher)
	tree.Each(func(key []byte, value Hasher) {
		m[hex.EncodeToString(key)] = value
	})
	comp := map[string]Hasher{
		hex.EncodeToString([]byte("apple")):     StringHasher("fruit"),
		hex.EncodeToString([]byte("crab")):      StringHasher("animal"),
		hex.EncodeToString([]byte("crabapple")): StringHasher("fruit"),
		hex.EncodeToString([]byte("banana")):    StringHasher("fruit"),
		hex.EncodeToString([]byte("guava")):     StringHasher("fruit"),
		hex.EncodeToString([]byte("guanabana")): StringHasher("city"),
	}
	if !reflect.DeepEqual(m, comp) {
		t.Errorf("should be equal!")
	}
	if !reflect.DeepEqual(tree.ToMap(), comp) {
		t.Errorf("should be equal!")
	}
	if old, existed := tree.Put(nil, StringHasher("nil"), 0); old != nil || existed {
		t.Error("should not exist yet")
	}
	if old, existed := tree.Put([]byte("nil"), nil, 0); old != nil || existed {
		t.Error("should not exist yet")
	}
	if value, _, existed := tree.Get(nil); !existed || value != StringHasher("nil") {
		t.Errorf("%v should contain %v => %v, got %v, %v", tree, nil, "nil", value, existed)
	}
	if value, _, existed := tree.Get([]byte("nil")); !existed || value != nil {
		t.Errorf("%v should contain %v => %v, got %v, %v", tree, "nil", nil, value, existed)
	}
	assertDelFailure(t, tree, "gua")
	assertSize(t, tree, 8)
	assertDelSuccess(t, tree, "apple", "fruit")
	assertSize(t, tree, 7)
	assertDelFailure(t, tree, "apple")
	assertSize(t, tree, 7)
	assertDelSuccess(t, tree, "crab", "animal")
	assertSize(t, tree, 6)
	assertDelFailure(t, tree, "crab")
	assertSize(t, tree, 6)
	assertDelSuccess(t, tree, "crabapple", "fruit")
	assertSize(t, tree, 5)
	assertDelFailure(t, tree, "crabapple")
	assertSize(t, tree, 5)
	assertDelSuccess(t, tree, "banana", "fruit")
	assertSize(t, tree, 4)
	assertDelFailure(t, tree, "banana")
	assertSize(t, tree, 4)
	assertDelSuccess(t, tree, "guava", "fruit")
	assertSize(t, tree, 3)
	assertDelFailure(t, tree, "guava")
	assertSize(t, tree, 3)
	assertDelSuccess(t, tree, "guanabana", "city")
	assertSize(t, tree, 2)
	assertDelFailure(t, tree, "guanabana")
	assertSize(t, tree, 2)
}

func benchTreeSync(b *testing.B, size, delta int) {
	b.StopTimer()
	tree1 := NewTree()
	tree2 := NewTree()
	var k []byte
	var v Hasher
	for i := 0; i < size; i++ {
		k = murmur.HashString(fmt.Sprint(i))
		v = StringHasher(fmt.Sprint(i))
		tree1.Put(k, v, 0)
		tree2.Put(k, v, 0)
	}
	var s *Sync
	for i := 0; i < b.N/delta; i++ {
		for j := 0; j < delta; j++ {
			tree2.Del(murmur.HashString(fmt.Sprint(j)))
		}
		b.StartTimer()
		s = NewSync(tree1, tree2)
		s.Run()
		b.StopTimer()
		if bytes.Compare(tree1.Hash(), tree2.Hash()) != 0 {
			b.Fatalf("%v != %v", tree1.Hash(), tree2.Hash())
		}
	}
}

func BenchmarkTreeSync10000_1(b *testing.B) {
	benchTreeSync(b, 10000, 1)
}
func BenchmarkTreeSync10000_10(b *testing.B) {
	benchTreeSync(b, 10000, 10)
}
func BenchmarkTreeSync10000_100(b *testing.B) {
	benchTreeSync(b, 10000, 100)
}
func BenchmarkTreeSync10000_1000(b *testing.B) {
	benchTreeSync(b, 10000, 1000)
}

func BenchmarkTreeSync100000_1(b *testing.B) {
	benchTreeSync(b, 100000, 1)
}
func BenchmarkTreeSync100000_10(b *testing.B) {
	benchTreeSync(b, 100000, 10)
}
func BenchmarkTreeSync100000_100(b *testing.B) {
	benchTreeSync(b, 100000, 100)
}
func BenchmarkTreeSync100000_1000(b *testing.B) {
	benchTreeSync(b, 100000, 1000)
}

func benchTree(b *testing.B, n int, put, get bool) {
	b.StopTimer()
	for len(benchmarkTestKeys) < n {
		benchmarkTestKeys = append(benchmarkTestKeys, murmur.HashString(fmt.Sprint(len(benchmarkTestKeys))))
		benchmarkTestValues = append(benchmarkTestValues, StringHasher(fmt.Sprint(len(benchmarkTestValues))))
	}
	for benchmarkTestTree.Size() < n {
		benchmarkTestTree.Put(benchmarkTestKeys[benchmarkTestTree.Size()], benchmarkTestValues[benchmarkTestTree.Size()], 0)
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
		if put {
			benchmarkTestTree.Put(k, v, 0)
		}
		if get {
			j, _, existed := benchmarkTestTree.Get(k)
			if j != v {
				b.Fatalf("%v should contain %v, but got %v, %v", benchmarkTestTree.Describe(), v, j, existed)
			}
		}
	}
}

func BenchmarkTreePut10(b *testing.B) {
	benchTree(b, 10, true, false)
}

func BenchmarkTreeGet10(b *testing.B) {
	benchTree(b, 10, false, true)
}

func BenchmarkTreePut100(b *testing.B) {
	benchTree(b, 100, true, false)
}

func BenchmarkTreeGet100(b *testing.B) {
	benchTree(b, 100, false, true)
}

func BenchmarkTreePut1000(b *testing.B) {
	benchTree(b, 1000, true, false)
}

func BenchmarkTreeGet1000(b *testing.B) {
	benchTree(b, 1000, false, true)
}

func BenchmarkTreePut10000(b *testing.B) {
	benchTree(b, 10000, true, false)
}

func BenchmarkTreeGet10000(b *testing.B) {
	benchTree(b, 10000, false, true)
}

func BenchmarkTreePut100000(b *testing.B) {
	benchTree(b, 100000, true, false)
}

func BenchmarkTreeGet1000000(b *testing.B) {
	benchTree(b, 1000000, false, true)
}

func BenchmarkTreePut1000000(b *testing.B) {
	benchTree(b, 1000000, true, false)
}

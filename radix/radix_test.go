
package radix

import (
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
	tree.Put([]byte{}, StringHasher("nil"))
	assertExistance(t, tree, "apple", "fruit")
	assertExistance(t, tree, "crab", "animal")
	assertExistance(t, tree, "crabapple", "fruit")
	assertExistance(t, tree, "banana", "fruit")
	assertExistance(t, tree, "guava", "fruit")
	assertExistance(t, tree, "guanabana", "city")
}
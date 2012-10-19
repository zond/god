package radix

import (
	"testing"
)

func assertExistance(t *testing.T, tree *Tree, k, v string) {
	if value, _, existed := tree.Get([]byte(k)); !existed || value != StringHasher(v) {
		t.Errorf("%v should contain %v => %v, got %v, %v", tree.Describe(), rip([]byte(k)), v, value, existed)
	}
}

func assertNewPut(t *testing.T, tree *Tree, k, v string) {
	assertNonExistance(t, tree, k)
	if value, existed := tree.Put([]byte(k), StringHasher(v), 0); existed || value != nil {
		t.Errorf("%v should not contain %v, got %v, %v", tree.Describe(), rip([]byte(k)), value, existed)
	}
	assertExistance(t, tree, k, v)
}

func assertOldPut(t *testing.T, tree *Tree, k, v, old string) {
	assertExistance(t, tree, k, old)
	if value, existed := tree.Put([]byte(k), StringHasher(v), 0); !existed || value != StringHasher(old) {
		t.Errorf("%v should contain %v => %v, got %v, %v", tree.Describe(), rip([]byte(k)), v, value, existed)
	}
	assertExistance(t, tree, k, v)
}

func assertDelSuccess(t *testing.T, tree *Tree, k, old string) {
	assertExistance(t, tree, k, old)
	if value, existed := tree.Del([]byte(k)); !existed || value != StringHasher(old) {
		t.Errorf("%v should contain %v => %v, got %v, %v", tree.Describe(), rip([]byte(k)), old, value, existed)
	}
	assertNonExistance(t, tree, k)
}

func assertDelFailure(t *testing.T, tree *Tree, k string) {
	assertNonExistance(t, tree, k)
	if value, existed := tree.Del([]byte(k)); existed || value != nil {
		t.Errorf("%v should not contain %v, got %v, %v", tree.Describe(), rip([]byte(k)), value, existed)
	}
	assertNonExistance(t, tree, k)
}

func assertNonExistance(t *testing.T, tree *Tree, k string) {
	if value, _, existed := tree.Get([]byte(k)); existed || value != nil {
		t.Errorf("%v should not contain %v, got %v, %v", tree, k, value, existed)
	}
}

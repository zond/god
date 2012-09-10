
package tree

import (
	"testing"
	"reflect"
)

func assertMappness(t *testing.T, tree *Tree, m map[string]Thing) {
	if !reflect.DeepEqual(tree.ToMap(), m) {
		t.Errorf("%v should be %v", tree, m)
	}
}

func TestTreeBasicOps(t *testing.T) {
	tree := new(Tree)
	m := make(map[string]Thing)
	assertMappness(t, tree, m)
	if old, existed := tree.Put([]byte("key"), "value"); old != nil || existed {
		t.Errorf("should not have existed")
	}
	m["key"] = "value"
	assertMappness(t, tree, m)
}


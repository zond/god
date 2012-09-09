
package tree

import (
	"testing"
)

func TestTreeBasicOps(t *testing.T) {
	tree := new(Tree)
	if old, existed := tree.Put([]byte("key"), "value"); old != nil || existed {
		t.Errorf("should not have existed")
	}
	t.Error(tree)
}


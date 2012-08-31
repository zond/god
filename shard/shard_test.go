
package shard

import (
	"testing"
)

func TestShardLoading(t *testing.T) {
	s, err := NewEmptyShard("test1")
	if err != nil {
		t.Errorf("while trying to create empty shard: %v", err)
	}
	r := &Response{}
	s.Perform(Operation{PUT, []string{"k", "v"}}, r)
	s, err = NewShard("test1")
	if err != nil {
		t.Errorf("while trying to load shard: %v", err)
	}
	s.Perform(Operation{GET, []string{"k"}}, r)
	if r.Result & OK != OK {
		t.Errorf("should be OK/k: %v", r)
	}
	if r.Parts[0] != "v" {
		t.Errorf("should be OK/k: %v", r)
	}
}

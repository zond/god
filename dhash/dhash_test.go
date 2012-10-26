package dhash

import (
	"../common"
	"../radix"
	"bytes"
	"fmt"
	"testing"
	"time"
)

func countHaving(t *testing.T, dhashes []*DHash, key, value []byte) (result int) {
	for _, d := range dhashes {
		if foundValue, _, existed := d.tree.Get(key); existed && bytes.Compare([]byte(foundValue.(radix.ByteHasher)), value) == 0 {
			result++
		}
	}
	return
}

func TestSyncClean(t *testing.T) {
	n := 6
	dhashes := make([]*DHash, n)
	for i := 0; i < n; i++ {
		dhashes[i] = NewDHash(fmt.Sprintf("127.0.0.1:%v", 9191+i))
		dhashes[i].MustStart()
	}
	for i := 1; i < n; i++ {
		dhashes[i].MustJoin("127.0.0.1:9191")
	}
	common.AssertWithin(t, func() (string, bool) {
		var nodes common.Remotes
		routes := make(map[string]bool)
		for _, dhash := range dhashes {
			common.Switch.Call(dhash.node.GetAddr(), "Node.Nodes", 0, &nodes)
			routes[nodes.Describe()] = true
		}
		return fmt.Sprint(routes), len(routes) == 1
	}, time.Second*10)
	dhashes[0].tree.Put([]byte{0}, radix.ByteHasher([]byte{0}), 0)
	common.AssertWithin(t, func() (string, bool) {
		having := countHaving(t, dhashes, []byte{0}, radix.ByteHasher([]byte{0}))
		return fmt.Sprint(having), having == common.Redundancy
	}, time.Second*10)
	for _, n := range dhashes {
		n.tree.Put([]byte{1}, radix.ByteHasher([]byte{1}), 0)
	}
	common.AssertWithin(t, func() (string, bool) {
		having := countHaving(t, dhashes, []byte{1}, []byte{1})
		return fmt.Sprint(having), having == common.Redundancy
	}, time.Second*10)
}

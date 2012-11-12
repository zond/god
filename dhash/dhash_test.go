package dhash

import (
	"../common"
	"../radix"
	"bytes"
	"fmt"
	"runtime"
	"testing"
	"time"
)

func countHaving(t *testing.T, dhashes []*Node, key, value []byte) (result int) {
	for _, d := range dhashes {
		if foundValue, _, existed := d.tree.Get(key); existed && bytes.Compare([]byte(foundValue.(radix.ByteHasher)), value) == 0 {
			result++
		}
	}
	return
}

func testStartup(t *testing.T, n, port int) (dhashes []*Node) {
	dhashes = make([]*Node, n)
	for i := 0; i < n; i++ {
		dhashes[i] = NewNode(fmt.Sprintf("127.0.0.1:%v", port+i))
		dhashes[i].MustStart()
	}
	for i := 1; i < n; i++ {
		dhashes[i].MustJoin(fmt.Sprintf("127.0.0.1:%v", port))
	}
	common.AssertWithin(t, func() (string, bool) {
		routes := make(map[string]bool)
		for _, dhash := range dhashes {
			routes[dhash.node.GetNodes().Describe()] = true
		}
		return fmt.Sprint(routes), len(routes) == 1
	}, time.Second*10)
	return
}

func testSync(t *testing.T, dhashes []*Node) {
	dhashes[0].tree.Put([]byte{0}, radix.ByteHasher([]byte{0}), 0)
	common.AssertWithin(t, func() (string, bool) {
		having := countHaving(t, dhashes, []byte{0}, radix.ByteHasher([]byte{0}))
		return fmt.Sprint(having), having == common.Redundancy
	}, time.Second*10)
}

func testClean(t *testing.T, dhashes []*Node) {
	for _, n := range dhashes {
		n.tree.Put([]byte{1}, radix.ByteHasher([]byte{1}), 0)
	}
	common.AssertWithin(t, func() (string, bool) {
		having := countHaving(t, dhashes, []byte{1}, []byte{1})
		return fmt.Sprint(having), having == common.Redundancy
	}, time.Second*10)
}

func testPut(t *testing.T, dhashes []*Node) {
	for index, n := range dhashes {
		n.Put(common.Item{Key: []byte{byte(index + 100)}, Value: radix.ByteHasher([]byte{byte(index + 100)})})
	}
	common.AssertWithin(t, func() (string, bool) {
		haves := make(map[int]bool)
		for index, _ := range dhashes {
			count := countHaving(t, dhashes, []byte{byte(index + 100)}, []byte{byte(index + 100)})
			haves[count] = true
		}
		return fmt.Sprint(haves), len(haves) == 1 && haves[common.Redundancy] == true
	}, time.Second*10)
}

func testFind(t *testing.T, dhashes []*Node) {
	dhashes[0].tree.Put([]byte{2}, radix.ByteHasher([]byte{2}), 0)
	common.AssertWithin(t, func() (string, bool) {
		having := make(map[bool]bool)
		for _, n := range dhashes {
			result := common.Item{}
			common.Switch.Call(n.node.GetAddr(), "DHash.Find", common.Item{Key: []byte{2}}, &result)
			having[result.Exists] = true
		}
		return fmt.Sprint(having), len(having) == 1 && having[true] == true
	}, time.Second*10)
}

func stopServers(servers []*Node) {
	for _, d := range servers {
		d.Stop()
	}
}

func TestAll(t *testing.T) {
	runtime.GOMAXPROCS(runtime.NumCPU())
	dhashes := testStartup(t, 6, 10191)
	testSync(t, dhashes)
	testClean(t, dhashes)
	testPut(t, dhashes)
	testFind(t, dhashes)
}

package dhash

import (
	"../client"
	"../common"
	"../murmur"
	"bytes"
	"fmt"
	"runtime"
	"testing"
	"time"
)

func TestClient(t *testing.T) {
	dhashes := testStartup(t, common.Redundancy*2, 11191)
	c := client.MustConn(dhashes[0].GetAddr())
	c.Start()
	testGetPutDel(t, c)
	testSubGetPutDel(t, c)
	testSubClear(t, c)
	testIndices(t, c)
	testDump(t, c)
	testSubDump(t, c)
	testNextPrev(t, c)
	testCounts(t, c)
	testNextPrevIndices(t, c)
	testSlices(t, c)
	testSliceIndices(t, c)
	testSliceLen(t, c)
}

func assertMirrored(t *testing.T, c *client.Conn, subTree []byte) {
	common.AssertWithin(t, func() (string, bool) {
		for _, n := range c.Nodes() {
			var conf common.Conf
			if err := n.Call("DHash.SubConfiguration", subTree, &conf); err != nil {
				return err.Error(), false
			}
			var s int
			if err := n.Call("DHash.SubSize", subTree, &s); err != nil {
				return err.Error(), false
			}
			if s > 0 {
				if conf.Data["mirrored"] != "yes" {
					return fmt.Sprint(n.String(), conf.Data), false
				}
			}
		}
		return "", true
	}, time.Second*10)
}

func testNextPrev(t *testing.T, c *client.Conn) {
	c.SPut([]byte("testNextPrev1"), []byte("v1"))
	c.SPut([]byte("testNextPrev2"), []byte("v2"))
	if k, v, e := c.Prev([]byte("testNextPrev2")); string(k) != "testNextPrev1" || string(v) != "v1" || !e {
		t.Errorf("wrong next")
	}
	if k, v, e := c.Next([]byte("testNextPrev1")); string(k) != "testNextPrev2" || string(v) != "v2" || !e {
		t.Errorf("wrong next")
	}
}

func testSubDump(t *testing.T, c *client.Conn) {
	ch, wa := c.SubDump([]byte("hest"))
	ch <- [2][]byte{[]byte("testSubDumpk1"), []byte("testSubDumpv1")}
	ch <- [2][]byte{[]byte("testSubDumpk2"), []byte("testSubDumpv2")}
	close(ch)
	wa.Wait()
	if val, ex := c.SubGet([]byte("hest"), []byte("testSubDumpk1")); !ex || bytes.Compare(val, []byte("testSubDumpv1")) != 0 {
		t.Errorf("wrong value")
	}
	if val, ex := c.SubGet([]byte("hest"), []byte("testSubDumpk2")); !ex || bytes.Compare(val, []byte("testSubDumpv2")) != 0 {
		t.Errorf("wrong value")
	}
}

func testDump(t *testing.T, c *client.Conn) {
	ch, wa := c.Dump()
	ch <- [2][]byte{[]byte("testDumpk1"), []byte("testDumpv1")}
	ch <- [2][]byte{[]byte("testDumpk2"), []byte("testDumpv2")}
	close(ch)
	wa.Wait()
	if val, ex := c.Get([]byte("testDumpk1")); !ex || bytes.Compare(val, []byte("testDumpv1")) != 0 {
		t.Errorf("wrong value")
	}
	if val, ex := c.Get([]byte("testDumpk2")); !ex || bytes.Compare(val, []byte("testDumpv2")) != 0 {
		t.Errorf("wrong value")
	}
}

func testSubGetPutDel(t *testing.T, c *client.Conn) {
	var key []byte
	var value []byte
	var subKey []byte
	for j := 0; j < 100; j++ {
		key = murmur.HashString(fmt.Sprint(j))
		for i := 0; i < 10; i++ {
			subKey = murmur.HashString(fmt.Sprint(i))
			value = murmur.HashString(fmt.Sprint(i))
			if v, e := c.SubGet(key, subKey); v != nil || e {
				t.Errorf("shouldn't exist")
			}
			c.SSubPut(key, subKey, value)
			if v, e := c.SubGet(key, subKey); bytes.Compare(value, v) != 0 || !e {
				t.Errorf("should exist, but got %v => %v, %v", key, v, e)
			}
			c.SSubDel(key, subKey)
			if v, e := c.SubGet(key, subKey); v != nil || e {
				t.Errorf("shouldn't exist, but got %v => %v, %v", key, v, e)
			}
		}
	}
}

func testSliceLen(t *testing.T, c *client.Conn) {
	var key []byte
	var value []byte
	subTree := []byte("jaguar2")
	c.SubAddConfiguration(subTree, "mirrored", "yes")
	for i := byte(1); i < 9; i++ {
		key = []byte{i}
		value = []byte{9 - i}
		c.SSubPut(subTree, key, value)
	}
	assertMirrored(t, c, subTree)
	assertItems(t, c.SliceLen(subTree, []byte{2}, true, 3), []byte{2, 3, 4}, []byte{7, 6, 5})
	assertItems(t, c.SliceLen(subTree, []byte{2}, false, 3), []byte{3, 4, 5}, []byte{6, 5, 4})
	assertItems(t, c.ReverseSliceLen(subTree, []byte{6}, true, 3), []byte{6, 5, 4}, []byte{3, 4, 5})
	assertItems(t, c.ReverseSliceLen(subTree, []byte{6}, false, 3), []byte{5, 4, 3}, []byte{4, 5, 6})
	assertItems(t, c.MirrorSliceLen(subTree, []byte{2}, true, 3), []byte{2, 3, 4}, []byte{7, 6, 5})
	assertItems(t, c.MirrorSliceLen(subTree, []byte{2}, false, 3), []byte{3, 4, 5}, []byte{6, 5, 4})
	assertItems(t, c.MirrorReverseSliceLen(subTree, []byte{6}, true, 3), []byte{6, 5, 4}, []byte{3, 4, 5})
	assertItems(t, c.MirrorReverseSliceLen(subTree, []byte{6}, false, 3), []byte{5, 4, 3}, []byte{4, 5, 6})
}

func testCounts(t *testing.T, c *client.Conn) {
	var key []byte
	var value []byte
	subTree := []byte("jaguar")
	c.SubAddConfiguration(subTree, "mirrored", "yes")
	for i := byte(1); i < 9; i++ {
		key = []byte{i}
		value = []byte{19 - i}
		c.SSubPut(subTree, key, value)
	}
	assertMirrored(t, c, subTree)
	for i := byte(0); i < 10; i++ {
		for j := byte(0); j < 10; j++ {
			wanted := common.Max(0, common.Min(int(j+1), 9)-common.Max(int(i), 1))
			found := c.Count([]byte("jaguar"), []byte{i}, []byte{j}, true, true)
			if found != wanted {
				t.Errorf("wrong count for %v-%v true true, wanted %v but found %v", i, j, wanted, found)
			}
			wanted = common.Max(0, common.Min(int(j), 9)-common.Max(int(i), 1))
			found = c.Count([]byte("jaguar"), []byte{i}, []byte{j}, true, false)
			if found != wanted {
				t.Errorf("wrong count for %v-%v true false, wanted %v but found %v", i, j, wanted, found)
			}
			wanted = common.Max(0, common.Min(int(j+1), 9)-common.Max(int(i+1), 1))
			found = c.Count([]byte("jaguar"), []byte{i}, []byte{j}, false, true)
			if found != wanted {
				t.Errorf("wrong count for %v-%v true false, wanted %v but found %v", i, j, wanted, found)
			}
			wanted = common.Max(0, common.Min(int(j), 9)-common.Max(int(i+1), 1))
			found = c.Count([]byte("jaguar"), []byte{i}, []byte{j}, false, false)
			if found != wanted {
				t.Errorf("wrong count for %v-%v false false, wanted %v but found %v", i, j, wanted, found)
			}

			wanted = common.Max(0, common.Min(int(j+10), 19)-common.Max(int(i+11), 11))
			found = c.MirrorCount([]byte("jaguar"), []byte{i + 10}, []byte{j + 10}, false, false)
			if found != wanted {
				t.Errorf("wrong count for mirror %v-%v false false, wanted %v but found %v", i+10, j+10, wanted, found)
			}
			wanted = common.Max(0, common.Min(int(j+10), 19)-common.Max(int(i+10), 11))
			found = c.MirrorCount([]byte("jaguar"), []byte{i + 10}, []byte{j + 10}, true, false)
			if found != wanted {
				t.Errorf("wrong count for mirror %v-%v true false, wanted %v but found %v", i+10, j+10, wanted, found)
			}
			wanted = common.Max(0, common.Min(int(j+11), 19)-common.Max(int(i+11), 11))
			found = c.MirrorCount([]byte("jaguar"), []byte{i + 10}, []byte{j + 10}, false, true)
			if found != wanted {
				t.Errorf("wrong count for mirror %v-%v false true, wanted %v but found %v", i+10, j+10, wanted, found)
			}
			wanted = common.Max(0, common.Min(int(j+11), 19)-common.Max(int(i+10), 11))
			found = c.MirrorCount([]byte("jaguar"), []byte{i + 10}, []byte{j + 10}, true, true)
			if found != wanted {
				t.Errorf("wrong count for mirror %v-%v true true, wanted %v but found %v", i+10, j+10, wanted, found)
			}
		}
	}
}

func assertItems(t *testing.T, items []common.Item, keys, values []byte) {
	_, file, line, _ := runtime.Caller(1)
	if len(items) == len(keys) && len(items) == len(values) {
		for index, item := range items {
			if string(item.Key) != string([]byte{keys[index]}) || string(item.Value) != string([]byte{values[index]}) {
				t.Errorf("%v:%v: wanted %v, %v but got %v", file, line, keys, values, items)
			}
		}
	} else {
		t.Errorf("%v:%v: wanted %v, %v but got %v", file, line, keys, values, items)
	}
}

func testSliceIndices(t *testing.T, c *client.Conn) {
	var key []byte
	var value []byte
	subTree := []byte("banan2")
	c.SubAddConfiguration(subTree, "mirrored", "yes")
	for i := byte(1); i < 9; i++ {
		key = []byte{i}
		value = []byte{9 - i}
		c.SSubPut(subTree, key, value)
	}
	assertMirrored(t, c, subTree)
	min := 2
	max := 5
	assertItems(t, c.SliceIndex(subTree, &min, &max), []byte{3, 4, 5, 6}, []byte{6, 5, 4, 3})
	assertItems(t, c.SliceIndex(subTree, nil, &max), []byte{1, 2, 3, 4, 5, 6}, []byte{8, 7, 6, 5, 4, 3})
	assertItems(t, c.SliceIndex(subTree, &min, nil), []byte{3, 4, 5, 6, 7, 8}, []byte{6, 5, 4, 3, 2, 1})
	assertItems(t, c.SliceIndex(subTree, nil, nil), []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{8, 7, 6, 5, 4, 3, 2, 1})
	assertItems(t, c.ReverseSliceIndex(subTree, &min, &max), []byte{6, 5, 4, 3}, []byte{3, 4, 5, 6})
	assertItems(t, c.ReverseSliceIndex(subTree, nil, &max), []byte{8, 7, 6, 5, 4, 3}, []byte{1, 2, 3, 4, 5, 6})
	assertItems(t, c.ReverseSliceIndex(subTree, &min, nil), []byte{6, 5, 4, 3, 2, 1}, []byte{3, 4, 5, 6, 7, 8})
	assertItems(t, c.ReverseSliceIndex(subTree, nil, nil), []byte{8, 7, 6, 5, 4, 3, 2, 1}, []byte{1, 2, 3, 4, 5, 6, 7, 8})
	assertItems(t, c.MirrorSliceIndex(subTree, &min, &max), []byte{3, 4, 5, 6}, []byte{6, 5, 4, 3})
	assertItems(t, c.MirrorSliceIndex(subTree, nil, &max), []byte{1, 2, 3, 4, 5, 6}, []byte{8, 7, 6, 5, 4, 3})
	assertItems(t, c.MirrorSliceIndex(subTree, &min, nil), []byte{3, 4, 5, 6, 7, 8}, []byte{6, 5, 4, 3, 2, 1})
	assertItems(t, c.MirrorSliceIndex(subTree, nil, nil), []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{8, 7, 6, 5, 4, 3, 2, 1})
	assertItems(t, c.MirrorReverseSliceIndex(subTree, &min, &max), []byte{6, 5, 4, 3}, []byte{3, 4, 5, 6})
	assertItems(t, c.MirrorReverseSliceIndex(subTree, nil, &max), []byte{8, 7, 6, 5, 4, 3}, []byte{1, 2, 3, 4, 5, 6})
	assertItems(t, c.MirrorReverseSliceIndex(subTree, &min, nil), []byte{6, 5, 4, 3, 2, 1}, []byte{3, 4, 5, 6, 7, 8})
	assertItems(t, c.MirrorReverseSliceIndex(subTree, nil, nil), []byte{8, 7, 6, 5, 4, 3, 2, 1}, []byte{1, 2, 3, 4, 5, 6, 7, 8})
}

func testSlices(t *testing.T, c *client.Conn) {
	var key []byte
	var value []byte
	subTree := []byte("banan")
	c.SubAddConfiguration(subTree, "mirrored", "yes")
	for i := byte(1); i < 9; i++ {
		key = []byte{i}
		value = []byte{9 - i}
		c.SSubPut(subTree, key, value)
	}
	assertMirrored(t, c, subTree)
	assertItems(t, c.MirrorReverseSlice(subTree, []byte{2}, []byte{5}, true, true), []byte{5, 4, 3, 2}, []byte{4, 5, 6, 7})
	assertItems(t, c.MirrorReverseSlice(subTree, []byte{2}, []byte{5}, true, false), []byte{4, 3, 2}, []byte{5, 6, 7})
	assertItems(t, c.MirrorReverseSlice(subTree, []byte{2}, []byte{5}, false, true), []byte{5, 4, 3}, []byte{4, 5, 6})
	assertItems(t, c.MirrorReverseSlice(subTree, []byte{2}, []byte{5}, false, false), []byte{4, 3}, []byte{5, 6})
	assertItems(t, c.MirrorSlice(subTree, []byte{2}, []byte{5}, true, true), []byte{2, 3, 4, 5}, []byte{7, 6, 5, 4})
	assertItems(t, c.MirrorSlice(subTree, []byte{2}, []byte{5}, true, false), []byte{2, 3, 4}, []byte{7, 6, 5})
	assertItems(t, c.MirrorSlice(subTree, []byte{2}, []byte{5}, false, true), []byte{3, 4, 5}, []byte{6, 5, 4})
	assertItems(t, c.MirrorSlice(subTree, []byte{2}, []byte{5}, false, false), []byte{3, 4}, []byte{6, 5})
	assertItems(t, c.ReverseSlice(subTree, []byte{2}, []byte{5}, true, true), []byte{5, 4, 3, 2}, []byte{4, 5, 6, 7})
	assertItems(t, c.ReverseSlice(subTree, []byte{2}, []byte{5}, true, false), []byte{4, 3, 2}, []byte{5, 6, 7})
	assertItems(t, c.ReverseSlice(subTree, []byte{2}, []byte{5}, false, true), []byte{5, 4, 3}, []byte{4, 5, 6})
	assertItems(t, c.ReverseSlice(subTree, []byte{2}, []byte{5}, false, false), []byte{4, 3}, []byte{5, 6})
	assertItems(t, c.Slice(subTree, []byte{2}, []byte{5}, true, true), []byte{2, 3, 4, 5}, []byte{7, 6, 5, 4})
	assertItems(t, c.Slice(subTree, []byte{2}, []byte{5}, true, false), []byte{2, 3, 4}, []byte{7, 6, 5})
	assertItems(t, c.Slice(subTree, []byte{2}, []byte{5}, false, true), []byte{3, 4, 5}, []byte{6, 5, 4})
	assertItems(t, c.Slice(subTree, []byte{2}, []byte{5}, false, false), []byte{3, 4}, []byte{6, 5})

	assertItems(t, c.MirrorReverseSlice(subTree, []byte{2}, nil, true, true), []byte{8, 7, 6, 5, 4, 3, 2}, []byte{1, 2, 3, 4, 5, 6, 7})
	assertItems(t, c.MirrorReverseSlice(subTree, []byte{2}, nil, false, true), []byte{8, 7, 6, 5, 4, 3}, []byte{1, 2, 3, 4, 5, 6})
	assertItems(t, c.MirrorSlice(subTree, []byte{2}, nil, true, true), []byte{2, 3, 4, 5, 6, 7, 8}, []byte{7, 6, 5, 4, 3, 2, 1})
	assertItems(t, c.MirrorSlice(subTree, []byte{2}, nil, false, true), []byte{3, 4, 5, 6, 7, 8}, []byte{6, 5, 4, 3, 2, 1})
	assertItems(t, c.ReverseSlice(subTree, []byte{2}, nil, true, true), []byte{8, 7, 6, 5, 4, 3, 2}, []byte{1, 2, 3, 4, 5, 6, 7})
	assertItems(t, c.ReverseSlice(subTree, []byte{2}, nil, false, true), []byte{8, 7, 6, 5, 4, 3}, []byte{1, 2, 3, 4, 5, 6})
	assertItems(t, c.Slice(subTree, []byte{2}, nil, true, true), []byte{2, 3, 4, 5, 6, 7, 8}, []byte{7, 6, 5, 4, 3, 2, 1})
	assertItems(t, c.Slice(subTree, []byte{2}, nil, false, true), []byte{3, 4, 5, 6, 7, 8}, []byte{6, 5, 4, 3, 2, 1})

	assertItems(t, c.MirrorReverseSlice(subTree, nil, []byte{5}, true, true), []byte{5, 4, 3, 2, 1}, []byte{4, 5, 6, 7, 8})
	assertItems(t, c.MirrorReverseSlice(subTree, nil, []byte{5}, true, false), []byte{4, 3, 2, 1}, []byte{5, 6, 7, 8})
	assertItems(t, c.MirrorSlice(subTree, nil, []byte{5}, true, true), []byte{1, 2, 3, 4, 5}, []byte{8, 7, 6, 5, 4})
	assertItems(t, c.MirrorSlice(subTree, nil, []byte{5}, true, false), []byte{1, 2, 3, 4}, []byte{8, 7, 6, 5})
	assertItems(t, c.ReverseSlice(subTree, nil, []byte{5}, true, true), []byte{5, 4, 3, 2, 1}, []byte{4, 5, 6, 7, 8})
	assertItems(t, c.ReverseSlice(subTree, nil, []byte{5}, true, false), []byte{4, 3, 2, 1}, []byte{5, 6, 7, 8})
	assertItems(t, c.Slice(subTree, nil, []byte{5}, true, true), []byte{1, 2, 3, 4, 5}, []byte{8, 7, 6, 5, 4})
	assertItems(t, c.Slice(subTree, nil, []byte{5}, true, false), []byte{1, 2, 3, 4}, []byte{8, 7, 6, 5})

	assertItems(t, c.MirrorReverseSlice(subTree, nil, nil, true, true), []byte{8, 7, 6, 5, 4, 3, 2, 1}, []byte{1, 2, 3, 4, 5, 6, 7, 8})
	assertItems(t, c.MirrorSlice(subTree, nil, nil, true, true), []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{8, 7, 6, 5, 4, 3, 2, 1})
	assertItems(t, c.ReverseSlice(subTree, nil, nil, true, true), []byte{8, 7, 6, 5, 4, 3, 2, 1}, []byte{1, 2, 3, 4, 5, 6, 7, 8})
	assertItems(t, c.Slice(subTree, nil, nil, true, true), []byte{1, 2, 3, 4, 5, 6, 7, 8}, []byte{8, 7, 6, 5, 4, 3, 2, 1})
}

func testNextPrevIndices(t *testing.T, c *client.Conn) {
	var key []byte
	var value []byte
	subTree := []byte("gris")
	c.SubAddConfiguration(subTree, "mirrored", "yes")
	for i := byte(1); i < 9; i++ {
		key = []byte{i}
		value = []byte{9 - i}
		c.SSubPut(subTree, key, value)
	}
	assertMirrored(t, c, subTree)
	if k, v, i, e := c.NextIndex(subTree, -1); string(k) != string([]byte{1}) || string(v) != string([]byte{8}) || i != 0 || !e {
		t.Errorf("wrong next index! wanted %v, %v, %v, %v but got %v, %v, %v, %v", 1, 8, 0, true, k, v, i, e)
	}
	if _, _, _, e := c.NextIndex(subTree, 7); e {
		t.Errorf("wrong next index! wanted %v but got %v", false, e)
	}
	if k, v, i, e := c.PrevIndex(subTree, 8); string(k) != string([]byte{8}) || string(v) != string([]byte{1}) || i != 7 || !e {
		t.Errorf("wrong prev index! wanted %v, %v, %v, %v but got %v, %v, %v, %v", 8, 1, 7, true, k, v, i, e)
	}
	if _, _, _, e := c.PrevIndex(subTree, 0); e {
		t.Errorf("wrong prev index! wanted %v but got %v", false, e)
	}

	for j := 0; j < 7; j++ {
		if k, v, i, e := c.NextIndex(subTree, j); string(k) != string([]byte{byte(j + 2)}) || string(v) != string([]byte{byte(7 - j)}) || i != j+1 || !e {
			t.Errorf("wrong next index for %v! wanted %v, %v, %v, %v but got %v, %v, %v, %v", j, j+2, 7-j, j+1, true, k, v, i, e)
		}
	}
	for j := 1; j < 8; j++ {
		if k, v, i, e := c.PrevIndex(subTree, j); string(k) != string([]byte{byte(j)}) || string(v) != string([]byte{byte(9 - j)}) || i != j-1 || !e {
			t.Errorf("wrong prev index for %v! wanted %v, %v, %v, %v but got %v, %v, %v, %v", j, j, 9-j, j-1, true, k, v, i, e)
		}
	}

	if k, v, i, e := c.MirrorNextIndex(subTree, -1); string(k) != string([]byte{1}) || string(v) != string([]byte{8}) || i != 0 || !e {
		t.Errorf("wrong next index! wanted %v, %v, %v, %v but got %v, %v, %v, %v", 1, 8, 0, true, k, v, i, e)
	}
	if _, _, _, e := c.MirrorNextIndex(subTree, 7); e {
		t.Errorf("wrong next index! wanted %v but got %v", false, e)
	}
	if k, v, i, e := c.MirrorPrevIndex(subTree, 8); string(k) != string([]byte{8}) || string(v) != string([]byte{1}) || i != 7 || !e {
		t.Errorf("wrong prev index! wanted %v, %v, %v, %v but got %v, %v, %v, %v", 8, 1, 7, true, k, v, i, e)
	}
	if _, _, _, e := c.MirrorPrevIndex(subTree, 0); e {
		t.Errorf("wrong prev index! wanted %v but got %v", false, e)
	}

	for j := 1; j < 8; j++ {
		if k, v, i, e := c.MirrorPrevIndex(subTree, j); string(k) != string([]byte{byte(j)}) || string(v) != string([]byte{byte(9 - j)}) || i != j-1 || !e {
			t.Errorf("wrong mirror next index for %v! wanted %v, %v, %v, %v but got %v, %v, %v, %v", j, j, 9-j, j-1, true, k, v, i, e)
		}
	}
	for j := 1; j < 8; j++ {
		if k, v, i, e := c.MirrorPrevIndex(subTree, j); string(k) != string([]byte{byte(j)}) || string(v) != string([]byte{byte(9 - j)}) || i != j-1 || !e {
			t.Errorf("wrong mirror prev index for %v! wanted %v, %v, %v, %v but got %v, %v, %v, %v", j, j, 9-j, j-1, true, k, v, i, e)
		}
	}
}

func testIndices(t *testing.T, c *client.Conn) {
	var key []byte
	var value []byte
	subTree := []byte("ko")
	c.SubAddConfiguration(subTree, "mirrored", "yes")
	for i := byte(1); i < 9; i++ {
		key = []byte{i}
		value = []byte{9 - i}
		c.SSubPut(subTree, key, value)
	}
	assertMirrored(t, c, subTree)
	if ind, ok := c.ReverseIndexOf(subTree, []byte{9}); ind != 0 || ok {
		t.Errorf("wrong index! wanted %v, %v but got %v, %v", 0, false, ind, ok)
	}
	if ind, ok := c.MirrorReverseIndexOf(subTree, []byte{9}); ind != 0 || ok {
		t.Errorf("wrong index! wanted %v, %v but got %v, %v", 0, false, ind, ok)
	}
	if ind, ok := c.ReverseIndexOf(subTree, []byte{0}); ind != 8 || ok {
		t.Errorf("wrong index! wanted %v, %v but got %v, %v", 8, false, ind, ok)
	}
	if ind, ok := c.MirrorReverseIndexOf(subTree, []byte{0}); ind != 8 || ok {
		t.Errorf("wrong index! wanted %v, %v but got %v, %v", 8, false, ind, ok)
	}
	if ind, ok := c.IndexOf(subTree, []byte{9}); ind != 8 || ok {
		t.Errorf("wrong index! wanted %v, %v but got %v, %v", 8, false, ind, ok)
	}
	if ind, ok := c.MirrorIndexOf(subTree, []byte{9}); ind != 8 || ok {
		t.Errorf("wrong index! wanted %v, %v but got %v, %v", 8, false, ind, ok)
	}
	if ind, ok := c.IndexOf(subTree, []byte{0}); ind != 0 || ok {
		t.Errorf("wrong index! wanted %v, %v but got %v, %v", 0, false, ind, ok)
	}
	if ind, ok := c.MirrorIndexOf(subTree, []byte{0}); ind != 0 || ok {
		t.Errorf("wrong index! wanted %v, %v but got %v, %v", 0, false, ind, ok)
	}
	for i := byte(1); i < 9; i++ {
		if ind, ok := c.MirrorIndexOf(subTree, []byte{i}); ind != int(i-1) || !ok {
			t.Errorf("wrong index! wanted %v, %v but got %v, %v", i-1, true, ind, ok)
		}
		if ind, ok := c.MirrorReverseIndexOf(subTree, []byte{i}); ind != int(8-i) || !ok {
			t.Errorf("wrong index! wanted %v, %v but got %v, %v", 9-i, true, ind, ok)
		}
		if ind, ok := c.IndexOf(subTree, []byte{i}); ind != int(i-1) || !ok {
			t.Errorf("wrong index! wanted %v, %v but got %v, %v", i-1, true, ind, ok)
		}
		if ind, ok := c.ReverseIndexOf(subTree, []byte{i}); ind != int(8-i) || !ok {
			t.Errorf("wrong index! wanted %v, %v but got %v, %v", 9-i, true, ind, ok)
		}
	}
}

func testSubClear(t *testing.T, c *client.Conn) {
	var key []byte
	var value []byte
	subTree := []byte("apa")
	for i := 0; i < 10; i++ {
		key = murmur.HashString(fmt.Sprint(i))
		value = murmur.HashString(fmt.Sprint(i))
		c.SSubPut(subTree, key, value)
	}
	if c.SubSize(subTree) != 10 {
		t.Errorf("wrong size")
	}
	c.SSubClear(subTree)
	if c.SubSize(subTree) != 0 {
		t.Errorf("wrong size")
	}
}

func testGetPutDel(t *testing.T, c *client.Conn) {
	var key []byte
	var value []byte
	for i := 0; i < 1000; i++ {
		key = murmur.HashString(fmt.Sprint(i))
		value = murmur.HashString(fmt.Sprint(i))
		if v, e := c.Get(key); v != nil || e {
			t.Errorf("shouldn't exist")
		}
		c.SPut(key, value)
		if v, e := c.Get(key); bytes.Compare(value, v) != 0 || !e {
			t.Errorf("should exist, but got %v => %v, %v", key, v, e)
		}
		c.SDel(key)
		if v, e := c.Get(key); v != nil || e {
			t.Errorf("shouldn't exist, but got %v => %v, %v", key, v, e)
		}
	}
}

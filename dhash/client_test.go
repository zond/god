package dhash

import (
	"../client"
	"../common"
	"../murmur"
	"bytes"
	"fmt"
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

func testIndices(t *testing.T, c *client.Conn) {
	var key []byte
	var value []byte
	subTree := []byte("ko")
	c.SubAddConfiguration(subTree, "mirrored", "yes")
	for i := byte(0); i < 10; i++ {
		key = []byte{i}
		value = []byte{9 - i}
		c.SSubPut(subTree, key, value)
	}
	// because the test runs pretty fast, the nodes are still restructuring when we run the actual test if we dont sleep a bit here
	time.Sleep(time.Second * 2)
	for i := byte(0); i < 10; i++ {
		if ind, ok := c.MirrorIndexOf(subTree, []byte{i}); ind != int(i) || !ok {
			t.Errorf("wrong index! wanted %v, %v but got %v, %v", i, true, ind, ok)
		}
		if ind, ok := c.MirrorReverseIndexOf(subTree, []byte{i}); ind != int(9-i) || !ok {
			t.Errorf("wrong index! wanted %v, %v but got %v, %v", 9-i, true, ind, ok)
		}
		if ind, ok := c.IndexOf(subTree, []byte{i}); ind != int(i) || !ok {
			t.Errorf("wrong index! wanted %v, %v but got %v, %v", i, true, ind, ok)
		}
		if ind, ok := c.ReverseIndexOf(subTree, []byte{i}); ind != int(9-i) || !ok {
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

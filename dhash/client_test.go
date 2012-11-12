package dhash

import (
	"../client"
	"../common"
	"../murmur"
	"bytes"
	"fmt"
	"testing"
)

func TestClient(t *testing.T) {
	dhashes := testStartup(t, common.Redundancy*2, 11191)
	c := client.MustConn(dhashes[0].GetAddr())
	c.Start()
	testGetPutDel(t, c)
	testSubGetPutDel(t, c)
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
			c.SubPut(key, subKey, value)
			if v, e := c.SubGet(key, subKey); bytes.Compare(value, v) != 0 || !e {
				t.Errorf("should exist, but got %v => %v, %v", key, v, e)
			}
			c.SubDel(key, subKey)
			if v, e := c.SubGet(key, subKey); v != nil || e {
				t.Errorf("shouldn't exist, but got %v => %v, %v", key, v, e)
			}
		}
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
		c.Put(key, value)
		if v, e := c.Get(key); bytes.Compare(value, v) != 0 || !e {
			t.Errorf("should exist, but got %v => %v, %v", key, v, e)
		}
		c.Del(key)
		if v, e := c.Get(key); v != nil || e {
			t.Errorf("shouldn't exist, but got %v => %v, %v", key, v, e)
		}
	}
}

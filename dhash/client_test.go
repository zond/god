package dhash

import (
	"../client"
	"../common"
	"bytes"
	"testing"
)

func TestClient(t *testing.T) {
	dhashes := testStartup(t, common.Redundancy*2, 11191)
	c := client.MustConn(dhashes[0].GetAddr())
	key := []byte("k")
	value := []byte("v")
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

package dhash

import (
	"../radix"
	"sync/atomic"
	"time"
)

type HashTreeItem struct {
	Key         []radix.Nibble
	SubKey      []radix.Nibble
	Version     int64
	SubVersion  int64
	Expected    int64
	SubExpected int64
	Value       []byte
	Exists      bool
}

type hashTreeServer Node

func (self *hashTreeServer) Hash(x int, result *[]byte) error {
	*result = (*Node)(self).tree.Hash()
	return nil
}
func (self *hashTreeServer) Finger(key []radix.Nibble, result *radix.Print) error {
	*result = *((*Node)(self).tree.Finger(key))
	return nil
}
func (self *hashTreeServer) GetVersion(key []radix.Nibble, result *HashTreeItem) error {
	*result = HashTreeItem{Key: key}
	result.Value, result.Version, result.Exists = (*Node)(self).tree.GetVersion(key)
	return nil
}
func (self *hashTreeServer) PutVersion(data HashTreeItem, changed *bool) error {
	atomic.StoreInt64(&(*Node)(self).lastSync, time.Now().UnixNano())
	*changed = (*Node)(self).tree.PutVersion(data.Key, data.Value, data.Expected, data.Version)
	return nil
}
func (self *hashTreeServer) DelVersion(data HashTreeItem, changed *bool) error {
	atomic.StoreInt64(&(*Node)(self).lastSync, time.Now().UnixNano())
	*changed = (*Node)(self).tree.DelVersion(data.Key, data.Expected)
	return nil
}
func (self *hashTreeServer) SubFinger(data HashTreeItem, result *radix.Print) error {
	*result = *((*Node)(self).tree.SubFinger(data.Key, data.SubKey, data.Expected))
	return nil
}
func (self *hashTreeServer) SubGetVersion(data HashTreeItem, result *HashTreeItem) error {
	*result = data
	result.Value, result.SubVersion, result.Exists = (*Node)(self).tree.SubGetVersion(data.Key, data.SubKey, data.Expected)
	return nil
}
func (self *hashTreeServer) SubPutVersion(data HashTreeItem, changed *bool) error {
	atomic.StoreInt64(&(*Node)(self).lastSync, time.Now().UnixNano())
	*changed = (*Node)(self).tree.SubPutVersion(data.Key, data.SubKey, data.Value, data.Expected, data.SubExpected, data.SubVersion)
	return nil
}
func (self *hashTreeServer) SubDelVersion(data HashTreeItem, changed *bool) error {
	atomic.StoreInt64(&(*Node)(self).lastSync, time.Now().UnixNano())
	*changed = (*Node)(self).tree.SubDelVersion(data.Key, data.SubKey, data.Expected, data.SubExpected)
	return nil
}

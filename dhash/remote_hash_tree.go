package dhash

import (
	"../common"
	"../radix"
)

type remoteHashTree common.Remote

func (self remoteHashTree) Hash() (result []byte) {
	common.Remote(self).Call("HashTree.Hash", 0, &result)
	return
}
func (self remoteHashTree) Finger(key []radix.Nibble) (result *radix.Print) {
	result = &radix.Print{}
	common.Remote(self).Call("HashTree.Finger", key, result)
	return
}
func (self remoteHashTree) GetTimestamp(key []radix.Nibble) (value []byte, timestamp int64, existed bool) {
	result := HashTreeItem{}
	common.Remote(self).Call("HashTree.GetTimestamp", key, &result)
	value, timestamp, existed = result.Value, result.Timestamp, result.Exists
	return
}
func (self remoteHashTree) PutTimestamp(key []radix.Nibble, value []byte, expected, timestamp int64) (changed bool) {
	data := HashTreeItem{
		Key:       key,
		Value:     value,
		Expected:  expected,
		Timestamp: timestamp,
	}
	common.Remote(self).Call("HashTree.PutTimestamp", data, &changed)
	return
}
func (self remoteHashTree) DelTimestamp(key []radix.Nibble, expected int64) (changed bool) {
	data := HashTreeItem{
		Key:      key,
		Expected: expected,
	}
	common.Remote(self).Call("HashTree.DelTimestamp", data, &changed)
	return
}
func (self remoteHashTree) SubFinger(key, subKey []radix.Nibble, expected int64) (result *radix.Print) {
	data := HashTreeItem{
		Key:      key,
		SubKey:   subKey,
		Expected: expected,
	}
	result = &radix.Print{}
	common.Remote(self).Call("HashTree.SubFinger", data, result)
	return
}
func (self remoteHashTree) SubGetTimestamp(key, subKey []radix.Nibble, expected int64) (value []byte, timestamp int64, existed bool) {
	data := HashTreeItem{
		Key:      key,
		SubKey:   subKey,
		Expected: expected,
	}
	common.Remote(self).Call("HashTree.SubGetTimestamp", data, &data)
	value, timestamp, existed = data.Value, data.SubTimestamp, data.Exists
	return
}
func (self remoteHashTree) SubPutTimestamp(key, subKey []radix.Nibble, value []byte, expected, subExpected, subTimestamp int64) (changed bool) {
	data := HashTreeItem{
		Key:          key,
		SubKey:       subKey,
		Value:        value,
		Expected:     expected,
		SubExpected:  subExpected,
		SubTimestamp: subTimestamp,
	}
	common.Remote(self).Call("HashTree.SubPutTimestamp", data, &changed)
	return
}
func (self remoteHashTree) SubDelTimestamp(key, subKey []radix.Nibble, expected, subExpected int64) (changed bool) {
	data := HashTreeItem{
		Key:         key,
		SubKey:      subKey,
		Expected:    expected,
		SubExpected: subExpected,
	}
	common.Remote(self).Call("HashTree.SubDelTimestamp", data, &changed)
	return
}

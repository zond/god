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
func (self remoteHashTree) GetVersion(key []radix.Nibble) (value radix.Hasher, version int64, existed bool) {
	result := HashTreeItem{}
	common.Remote(self).Call("HashTree.GetVersion", key, &result)
	value, version, existed = radix.ByteHasher(result.Value), result.Version, result.Exists
	return
}
func (self remoteHashTree) PutVersion(key []radix.Nibble, value radix.Hasher, expected, version int64) {
	data := HashTreeItem{
		Key:      key,
		Value:    []byte(value.(radix.ByteHasher)),
		Expected: expected,
		Version:  version,
	}
	var x int
	common.Remote(self).Call("HashTree.PutVersion", data, &x)
}
func (self remoteHashTree) DelVersion(key []radix.Nibble, expected int64) {
	data := HashTreeItem{
		Key:      key,
		Expected: expected,
	}
	var x int
	common.Remote(self).Call("HashTree.DelVersion", data, &x)
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
func (self remoteHashTree) SubGetVersion(key, subKey []radix.Nibble, expected int64) (value radix.Hasher, version int64, existed bool) {
	data := HashTreeItem{
		Key:      key,
		SubKey:   subKey,
		Expected: expected,
	}
	common.Remote(self).Call("HashTree.SubGetVersion", data, &data)
	value, version, existed = radix.ByteHasher(data.Value), data.SubVersion, data.Exists
	return
}
func (self remoteHashTree) SubPutVersion(key, subKey []radix.Nibble, value radix.Hasher, expected, subExpected, subVersion int64) {
	data := HashTreeItem{
		Key:         key,
		SubKey:      subKey,
		Value:       []byte(value.(radix.ByteHasher)),
		Expected:    expected,
		SubExpected: subExpected,
		SubVersion:  subVersion,
	}
	var x int
	common.Remote(self).Call("HashTree.SubPutVersion", data, &x)
}
func (self remoteHashTree) SubDelVersion(key, subKey []radix.Nibble, expected, subExpected int64) {
	data := HashTreeItem{
		Key:         key,
		SubKey:      subKey,
		Expected:    expected,
		SubExpected: subExpected,
	}
	var x int
	common.Remote(self).Call("HashTree.SubDelVersion", data, &x)
}

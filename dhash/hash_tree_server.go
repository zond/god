package dhash

import (
	"../radix"
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

type hashTreeServer radix.Tree

func (self *hashTreeServer) Hash(x int, result *[]byte) error {
	*result = (*radix.Tree)(self).Hash()
	return nil
}
func (self *hashTreeServer) Finger(key []radix.Nibble, result *radix.Print) error {
	*result = *((*radix.Tree)(self).Finger(key))
	return nil
}
func (self *hashTreeServer) GetVersion(key []radix.Nibble, result *HashTreeItem) error {
	*result = HashTreeItem{Key: key}
	if value, version, exists := (*radix.Tree)(self).GetVersion(key); exists {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			result.Value, result.Version, result.Exists = []byte(byteHasher), version, exists
		}
	}
	return nil
}
func (self *hashTreeServer) PutVersion(data HashTreeItem, changed *bool) error {
	*changed = (*radix.Tree)(self).PutVersion(data.Key, radix.ByteHasher(data.Value), data.Expected, data.Version)
	return nil
}
func (self *hashTreeServer) DelVersion(data HashTreeItem, changed *bool) error {
	*changed = (*radix.Tree)(self).DelVersion(data.Key, data.Expected)
	return nil
}
func (self *hashTreeServer) SubFinger(data HashTreeItem, result *radix.Print) error {
	*result = *((*radix.Tree)(self).SubFinger(data.Key, data.SubKey, data.Expected))
	return nil
}
func (self *hashTreeServer) SubGetVersion(data HashTreeItem, result *HashTreeItem) error {
	*result = data
	if value, version, exists := (*radix.Tree)(self).SubGetVersion(data.Key, data.SubKey, data.Expected); exists {
		if byteHasher, ok := value.(radix.ByteHasher); ok {
			result.Value, result.SubVersion, result.Exists = []byte(byteHasher), version, exists
		}
	}
	return nil
}
func (self *hashTreeServer) SubPutVersion(data HashTreeItem, changed *bool) error {
	*changed = (*radix.Tree)(self).SubPutVersion(data.Key, data.SubKey, radix.ByteHasher(data.Value), data.Expected, data.SubExpected, data.SubVersion)
	return nil
}
func (self *hashTreeServer) SubDelVersion(data HashTreeItem, changed *bool) error {
	*changed = (*radix.Tree)(self).SubDelVersion(data.Key, data.SubKey, data.Expected, data.SubExpected)
	return nil
}

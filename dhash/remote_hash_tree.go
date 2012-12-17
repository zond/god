package dhash

import (
	"../common"
	"../radix"
)

type remoteHashTree common.Remote

func (self remoteHashTree) Configuration() (conf map[string]string, timestamp int64) {
	var result common.Conf
	if err := common.Remote(self).Call("DHash.Configuration", 0, &result); err != nil {
		conf = make(map[string]string)
	} else {
		conf, timestamp = result.Data, result.Timestamp
	}
	return
}
func (self remoteHashTree) SubConfiguration(key []byte) (conf map[string]string, timestamp int64) {
	var result common.Conf
	if err := common.Remote(self).Call("DHash.SubConfiguration", key, &result); err != nil {
		conf = make(map[string]string)
	} else {
		conf, timestamp = result.Data, result.Timestamp
	}
	return
}
func (self remoteHashTree) Configure(conf map[string]string, timestamp int64) {
	var x int
	common.Remote(self).Call("HashTree.Configure", common.Conf{
		Data:      conf,
		Timestamp: timestamp,
	}, &x)
}
func (self remoteHashTree) SubConfigure(key []byte, conf map[string]string, timestamp int64) {
	var x int
	common.Remote(self).Call("HashTree.SubConfigure", common.Conf{
		TreeKey:   key,
		Data:      conf,
		Timestamp: timestamp,
	}, &x)
}
func (self remoteHashTree) Hash() (result []byte) {
	common.Remote(self).Call("HashTree.Hash", 0, &result)
	return
}
func (self remoteHashTree) Finger(key []radix.Nibble) (result *radix.Print) {
	result = &radix.Print{}
	common.Remote(self).Call("HashTree.Finger", key, result)
	return
}
func (self remoteHashTree) GetTimestamp(key []radix.Nibble) (value []byte, timestamp int64, present bool) {
	result := HashTreeItem{}
	common.Remote(self).Call("HashTree.GetTimestamp", key, &result)
	value, timestamp, present = result.Value, result.Timestamp, result.Exists
	return
}
func (self remoteHashTree) PutTimestamp(key []radix.Nibble, value []byte, present bool, expected, timestamp int64) (changed bool) {
	data := HashTreeItem{
		Key:       key,
		Value:     value,
		Exists:    present,
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
func (self remoteHashTree) SubFinger(key, subKey []radix.Nibble) (result *radix.Print) {
	data := HashTreeItem{
		Key:    key,
		SubKey: subKey,
	}
	result = &radix.Print{}
	common.Remote(self).Call("HashTree.SubFinger", data, result)
	return
}
func (self remoteHashTree) SubGetTimestamp(key, subKey []radix.Nibble) (value []byte, timestamp int64, present bool) {
	data := HashTreeItem{
		Key:    key,
		SubKey: subKey,
	}
	common.Remote(self).Call("HashTree.SubGetTimestamp", data, &data)
	value, timestamp, present = data.Value, data.Timestamp, data.Exists
	return
}
func (self remoteHashTree) SubPutTimestamp(key, subKey []radix.Nibble, value []byte, present bool, subExpected, subTimestamp int64) (changed bool) {
	data := HashTreeItem{
		Key:       key,
		SubKey:    subKey,
		Value:     value,
		Exists:    present,
		Expected:  subExpected,
		Timestamp: subTimestamp,
	}
	common.Remote(self).Call("HashTree.SubPutTimestamp", data, &changed)
	return
}
func (self remoteHashTree) SubDelTimestamp(key, subKey []radix.Nibble, subExpected int64) (changed bool) {
	data := HashTreeItem{
		Key:      key,
		SubKey:   subKey,
		Expected: subExpected,
	}
	common.Remote(self).Call("HashTree.SubDelTimestamp", data, &changed)
	return
}

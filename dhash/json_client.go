package dhash

import (
	"../common"
	"../setop"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
)

type jsonClient string

func (self jsonClient) call(action string, params, result interface{}) {
	client := new(http.Client)
	buf := new(bytes.Buffer)
	err := json.NewEncoder(buf).Encode(params)
	if err != nil {
		panic(err)
	}
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%v/rpc/DHash.%v", self, action), buf)
	if err != nil {
		panic(err)
	}
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	err = json.NewDecoder(resp.Body).Decode(result)
	if err != nil {
		panic(err)
	}
	resp.Body.Close()
}
func (self jsonClient) SSubPut(key, subKey, value []byte) {
	var x int
	item := common.Item{
		Key:    key,
		SubKey: subKey,
		Value:  value,
		Sync:   true,
	}
	self.call("SubPut", item, &x)
}
func (self jsonClient) SubPut(key, subKey, value []byte) {
	var x int
	item := common.Item{
		Key:    key,
		SubKey: subKey,
		Value:  value,
	}
	self.call("SubPut", item, &x)
}
func (self jsonClient) SPut(key, value []byte) {
	var x int
	item := common.Item{
		Key:   key,
		Value: value,
		Sync:  true,
	}
	self.call("Put", item, &x)
}
func (self jsonClient) Put(key, value []byte) {
	var x int
	item := common.Item{
		Key:   key,
		Value: value,
	}
	self.call("Put", item, &x)
}
func (self jsonClient) SubClear(key []byte) {
	var x int
	item := common.Item{
		Key: key,
	}
	self.call("SubClear", item, &x)
}
func (self jsonClient) SSubClear(key []byte) {
	var x int
	item := common.Item{
		Key:  key,
		Sync: true,
	}
	self.call("SubClear", item, &x)
}
func (self jsonClient) SubDel(key, subKey []byte) {
	var x int
	item := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	self.call("SubDel", item, &x)
}
func (self jsonClient) SSubDel(key, subKey []byte) {
	var x int
	item := common.Item{
		Key:    key,
		SubKey: subKey,
		Sync:   true,
	}
	self.call("SubDel", item, &x)
}
func (self jsonClient) SDel(key []byte) {
	var x int
	item := common.Item{
		Key:  key,
		Sync: true,
	}
	self.call("Del", item, &x)
}
func (self jsonClient) Del(key []byte) {
	var x int
	item := common.Item{
		Key: key,
	}
	self.call("Del", item, &x)
}
func (self jsonClient) MirrorReverseIndexOf(key, subKey []byte) (index int, existed bool) {
	item := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	var result common.Index
	self.call("MirrorReverseIndexOf", item, &result)
	return result.N, result.Existed
}
func (self jsonClient) MirrorIndexOf(key, subKey []byte) (index int, existed bool) {
	item := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	var result common.Index
	self.call("MirrorIndexOf", item, &result)
	return result.N, result.Existed
}
func (self jsonClient) ReverseIndexOf(key, subKey []byte) (index int, existed bool) {
	item := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	var result common.Index
	self.call("ReverseIndexOf", item, &result)
	return result.N, result.Existed
}
func (self jsonClient) IndexOf(key, subKey []byte) (index int, existed bool) {
	item := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	var result common.Index
	self.call("IndexOf", item, &result)
	return result.N, result.Existed
}
func (self jsonClient) Next(key []byte) (nextKey, nextValue []byte, existed bool) {
	item := common.Item{
		Key: key,
	}
	var result common.Item
	self.call("Next", item, &result)
	return result.Key, result.Value, result.Exists
}
func (self jsonClient) Prev(key []byte) (prevKey, prevValue []byte, existed bool) {
	item := common.Item{
		Key: key,
	}
	var result common.Item
	self.call("Prev", item, &result)
	return result.Key, result.Value, result.Exists
}
func (self jsonClient) MirrorCount(key, min, max []byte, mininc, maxinc bool) (result int) {
	item := common.Range{
		Key:    key,
		Min:    min,
		Max:    max,
		MinInc: mininc,
		MaxInc: maxinc,
	}
	self.call("MirrorCount", item, &result)
	return result
}
func (self jsonClient) Count(key, min, max []byte, mininc, maxinc bool) (result int) {
	item := common.Range{
		Key:    key,
		Min:    min,
		Max:    max,
		MinInc: mininc,
		MaxInc: maxinc,
	}
	self.call("Count", item, &result)
	return result
}
func (self jsonClient) MirrorNextIndex(key []byte, index int) (foundKey, foundValue []byte, foundIndex int, existed bool) {
	item := common.Item{
		Key:   key,
		Index: index,
	}
	var result common.Item
	self.call("MirrorNextIndex", item, &result)
	return result.Key, result.Value, result.Index, result.Exists
}
func (self jsonClient) MirrorPrevIndex(key []byte, index int) (foundKey, foundValue []byte, foundIndex int, existed bool) {
	item := common.Item{
		Key:   key,
		Index: index,
	}
	var result common.Item
	self.call("MirrorPrevIndex", item, &result)
	return result.Key, result.Value, result.Index, result.Exists
}
func (self jsonClient) NextIndex(key []byte, index int) (foundKey, foundValue []byte, foundIndex int, existed bool) {
	item := common.Item{
		Key:   key,
		Index: index,
	}
	var result common.Item
	self.call("NextIndex", item, &result)
	return result.Key, result.Value, result.Index, result.Exists
}
func (self jsonClient) PrevIndex(key []byte, index int) (foundKey, foundValue []byte, foundIndex int, existed bool) {
	item := common.Item{
		Key:   key,
		Index: index,
	}
	var result common.Item
	self.call("PrevIndex", item, &result)
	return result.Key, result.Value, result.Index, result.Exists
}
func (self jsonClient) MirrorReverseSliceIndex(key []byte, min, max *int) (result []common.Item) {
	var mi int
	var ma int
	if min != nil {
		mi = *min
	}
	if max != nil {
		ma = *max
	}
	item := common.Range{
		Key:      key,
		MinIndex: mi,
		MaxIndex: ma,
		MinInc:   min != nil,
		MaxInc:   max != nil,
	}
	self.call("MirrorReverseSliceIndex", item, &result)
	return result
}
func (self jsonClient) MirrorSliceIndex(key []byte, min, max *int) (result []common.Item) {
	var mi int
	var ma int
	if min != nil {
		mi = *min
	}
	if max != nil {
		ma = *max
	}
	item := common.Range{
		Key:      key,
		MinIndex: mi,
		MaxIndex: ma,
		MinInc:   min != nil,
		MaxInc:   max != nil,
	}
	self.call("MirrorSliceIndex", item, &result)
	return result
}
func (self jsonClient) MirrorReverseSlice(key, min, max []byte, mininc, maxinc bool) (result []common.Item) {
	item := common.Range{
		Key:    key,
		Min:    min,
		Max:    max,
		MinInc: mininc,
		MaxInc: maxinc,
	}
	self.call("MirrorReverseSlice", item, &result)
	return result
}
func (self jsonClient) MirrorSlice(key, min, max []byte, mininc, maxinc bool) (result []common.Item) {
	item := common.Range{
		Key:    key,
		Min:    min,
		Max:    max,
		MinInc: mininc,
		MaxInc: maxinc,
	}
	self.call("MirrorSlice", item, &result)
	return result
}
func (self jsonClient) MirrorSliceLen(key, min []byte, mininc bool, maxRes int) (result []common.Item) {
	item := common.Range{
		Key:    key,
		Min:    min,
		MinInc: mininc,
		Len:    maxRes,
	}
	self.call("MirrorSliceLen", item, &result)
	return result
}
func (self jsonClient) MirrorReverseSliceLen(key, max []byte, maxinc bool, maxRes int) (result []common.Item) {
	item := common.Range{
		Key:    key,
		Max:    max,
		MaxInc: maxinc,
		Len:    maxRes,
	}
	self.call("MirrorReverseSliceLen", item, &result)
	return result
}
func (self jsonClient) ReverseSliceIndex(key []byte, min, max *int) (result []common.Item) {
	var mi int
	var ma int
	if min != nil {
		mi = *min
	}
	if max != nil {
		ma = *max
	}
	item := common.Range{
		Key:      key,
		MinIndex: mi,
		MaxIndex: ma,
		MinInc:   min != nil,
		MaxInc:   max != nil,
	}
	self.call("ReverseSliceIndex", item, &result)
	return result
}
func (self jsonClient) SliceIndex(key []byte, min, max *int) (result []common.Item) {
	var mi int
	var ma int
	if min != nil {
		mi = *min
	}
	if max != nil {
		ma = *max
	}
	item := common.Range{
		Key:      key,
		MinIndex: mi,
		MaxIndex: ma,
		MinInc:   min != nil,
		MaxInc:   max != nil,
	}
	self.call("SliceIndex", item, &result)
	return result
}
func (self jsonClient) ReverseSlice(key, min, max []byte, mininc, maxinc bool) (result []common.Item) {
	item := common.Range{
		Key:    key,
		Min:    min,
		Max:    max,
		MinInc: mininc,
		MaxInc: maxinc,
	}
	self.call("ReverseSlice", item, &result)
	return result
}
func (self jsonClient) Slice(key, min, max []byte, mininc, maxinc bool) (result []common.Item) {
	item := common.Range{
		Key:    key,
		Min:    min,
		Max:    max,
		MinInc: mininc,
		MaxInc: maxinc,
	}
	self.call("Slice", item, &result)
	return result
}
func (self jsonClient) SliceLen(key, min []byte, mininc bool, maxRes int) (result []common.Item) {
	item := common.Range{
		Key:    key,
		Min:    min,
		MinInc: mininc,
		Len:    maxRes,
	}
	self.call("SliceLen", item, &result)
	return result
}
func (self jsonClient) ReverseSliceLen(key, max []byte, maxinc bool, maxRes int) (result []common.Item) {
	item := common.Range{
		Key:    key,
		Max:    max,
		MaxInc: maxinc,
		Len:    maxRes,
	}
	self.call("ReverseSliceLen", item, &result)
	return result
}
func (self jsonClient) SubMirrorPrev(key, subKey []byte) (prevKey, prevValue []byte, existed bool) {
	item := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	var result common.Item
	self.call("SubMirrorPrev", item, &result)
	return result.Key, result.Value, result.Exists
}
func (self jsonClient) SubMirrorNext(key, subKey []byte) (nextKey, nextValue []byte, existed bool) {
	item := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	var result common.Item
	self.call("SubMirrorNext", item, &result)
	return result.Key, result.Value, result.Exists
}
func (self jsonClient) SubPrev(key, subKey []byte) (prevKey, prevValue []byte, existed bool) {
	item := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	var result common.Item
	self.call("SubPrev", item, &result)
	return result.Key, result.Value, result.Exists
}
func (self jsonClient) SubNext(key, subKey []byte) (nextKey, nextValue []byte, existed bool) {
	item := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	var result common.Item
	self.call("SubNext", item, &result)
	return result.Key, result.Value, result.Exists
}
func (self jsonClient) MirrorLast(key []byte) (lastKey, lastValue []byte, existed bool) {
	item := common.Item{
		Key: key,
	}
	var result common.Item
	self.call("MirrorLast", item, &result)
	return result.Key, result.Value, result.Exists
}
func (self jsonClient) MirrorFirst(key []byte) (firstKey, firstValue []byte, existed bool) {
	item := common.Item{
		Key: key,
	}
	var result common.Item
	self.call("MirrorFirst", item, &result)
	return result.Key, result.Value, result.Exists
}
func (self jsonClient) Last(key []byte) (lastKey, lastValue []byte, existed bool) {
	item := common.Item{
		Key: key,
	}
	var result common.Item
	self.call("Last", item, &result)
	return result.Key, result.Value, result.Exists
}
func (self jsonClient) First(key []byte) (firstKey, firstValue []byte, existed bool) {
	item := common.Item{
		Key: key,
	}
	var result common.Item
	self.call("First", item, &result)
	return result.Key, result.Value, result.Exists
}
func (self jsonClient) SubGet(key, subKey []byte) (value []byte, existed bool) {
	item := common.Item{
		Key:    key,
		SubKey: subKey,
	}
	var result common.Item
	self.call("SubGet", item, &result)
	return result.Value, result.Exists
}
func (self jsonClient) Get(key []byte) (value []byte, existed bool) {
	item := common.Item{
		Key: key,
	}
	var result common.Item
	self.call("Get", item, &result)
	return result.Value, result.Exists
}
func (self jsonClient) SubSize(key []byte) (result int) {
	self.call("SubSize", key, &result)
	return result
}
func (self jsonClient) Size() (result int) {
	self.call("Size", 0, &result)
	return result
}
func (self jsonClient) SetExpression(expr setop.SetExpression) (result []setop.SetOpResult) {
	self.call("SetExpression", expr, &result)
	return
}
func (self jsonClient) SubAddConfiguration(treeKey []byte, key, value string) {
	conf := common.ConfItem{
		TreeKey: treeKey,
		Key:     key,
		Value:   value,
	}
	var x int
	self.call("SubAddConfiguration", conf, &x)
}

package dhash

import (
	"github.com/zond/god/common"
	"github.com/zond/god/setop"
)

type Nothing struct{}
type SubValue struct {
	Key    []byte
	SubKey []byte
	Value  []byte
	Sync   bool
}
type SubKeyOp struct {
	Key    []byte
	SubKey []byte
	Sync   bool
}
type SubKeyReq struct {
	Key    []byte
	SubKey []byte
}
type SubIndex struct {
	Key   []byte
	Index int
}
type Value struct {
	Key   []byte
	Value []byte
	Sync  bool
}
type KeyOp struct {
	Key  []byte
	Sync bool
}
type KeyReq struct {
	Key []byte
}
type KeyRange struct {
	Key    []byte
	Min    []byte
	Max    []byte
	MinInc bool
	MaxInc bool
}
type IndexRange struct {
	Key      []byte
	MinIndex *int
	MaxIndex *int
}
type PageRange struct {
	Key     []byte
	From    []byte
	FromInc bool
	Len     int
}
type SubConf struct {
	TreeKey []byte
	Key     string
	Value   string
}
type Conf struct {
	Key   string
	Value string
}

type JSONApi Node

func (self *JSONApi) forwardUnlessMe(cmd string, key []byte, in, out interface{}) (forwarded bool, err error) {
	succ := (*Node)(self).node.GetSuccessorFor(key)
	if succ.Addr != (*Node)(self).node.GetAddr() {
		forwarded, err = true, succ.Call(cmd, in, out)
	}
	return
}

func (self *JSONApi) Kill(x Nothing, y *Nothing) error {
	(*Node)(self).Kill()
	return nil
}
func (self *JSONApi) Clear(x Nothing, y *Nothing) error {
	(*Node)(self).Clear()
	return nil
}
func (self *JSONApi) SubDel(d SubKeyOp, n *Nothing) error {
	data := common.Item{
		Key:    d.Key,
		SubKey: d.SubKey,
		Sync:   d.Sync,
	}
	var x int
	if f, e := self.forwardUnlessMe("DHash.SubDel", data.Key, data, &x); f {
		return e
	}
	return (*Node)(self).SubDel(data)
}
func (self *JSONApi) SubClear(d SubKeyOp, n *Nothing) error {
	data := common.Item{
		Key:    d.Key,
		SubKey: d.SubKey,
		Sync:   d.Sync,
	}
	var x int
	if f, e := self.forwardUnlessMe("DHash.SubClear", data.Key, data, &x); f {
		return e
	}
	return (*Node)(self).SubClear(data)
}
func (self *JSONApi) SubPut(d SubValue, n *Nothing) error {
	data := common.Item{
		Key:    d.Key,
		SubKey: d.SubKey,
		Value:  d.Value,
		Sync:   d.Sync,
	}
	var x int
	if f, e := self.forwardUnlessMe("DHash.SubPut", data.Key, data, &x); f {
		return e
	}
	return (*Node)(self).SubPut(data)
}
func (self *JSONApi) Del(d KeyOp, n *Nothing) error {
	data := common.Item{
		Key:  d.Key,
		Sync: d.Sync,
	}
	var x int
	if f, e := self.forwardUnlessMe("DHash.Del", data.Key, data, &x); f {
		return e
	}
	return (*Node)(self).Del(data)
}
func (self *JSONApi) Put(d Value, n *Nothing) error {
	data := common.Item{
		Key:   d.Key,
		Value: d.Value,
		Sync:  d.Sync,
	}
	var x int
	if f, e := self.forwardUnlessMe("DHash.Put", data.Key, data, &x); f {
		return e
	}
	return (*Node)(self).Put(data)
}
func (self *JSONApi) MirrorCount(kr KeyRange, result *int) error {
	r := common.Range{
		Key:    kr.Key,
		Min:    kr.Min,
		Max:    kr.Max,
		MinInc: kr.MinInc,
		MaxInc: kr.MaxInc,
	}
	if f, e := self.forwardUnlessMe("DHash.MirrorCount", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).MirrorCount(r, result)
}
func (self *JSONApi) Count(kr KeyRange, result *int) error {
	r := common.Range{
		Key:    kr.Key,
		Min:    kr.Min,
		Max:    kr.Max,
		MinInc: kr.MinInc,
		MaxInc: kr.MaxInc,
	}
	if f, e := self.forwardUnlessMe("DHash.Count", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).Count(r, result)
}
func (self *JSONApi) Next(kr KeyReq, result *common.Item) error {
	k, v, e := (*Node)(self).client().Next(kr.Key)
	*result = common.Item{
		Key:    k,
		Value:  v,
		Exists: e,
	}
	return nil
}
func (self *JSONApi) Prev(kr KeyReq, result *common.Item) error {
	k, v, e := (*Node)(self).client().Prev(kr.Key)
	*result = common.Item{
		Key:    k,
		Value:  v,
		Exists: e,
	}
	return nil
}
func (self *JSONApi) SubGet(k SubKeyReq, result *common.Item) error {
	data := common.Item{
		Key:    k.Key,
		SubKey: k.SubKey,
	}
	if f, e := self.forwardUnlessMe("DHash.SubGet", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).SubGet(data, result)
}
func (self *JSONApi) Get(k KeyReq, result *common.Item) error {
	data := common.Item{
		Key: k.Key,
	}
	if f, e := self.forwardUnlessMe("DHash.Get", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).Get(data, result)
}
func (self *JSONApi) Size(x Nothing, result *int) error {
	*result = (*Node)(self).Size()
	return nil
}
func (self *JSONApi) SubSize(key []byte, result *int) error {
	if f, e := self.forwardUnlessMe("DHash.SubSize", key, key, result); f {
		return e
	}
	return (*Node)(self).SubSize(key, result)
}
func (self *JSONApi) Owned(x Nothing, result *int) error {
	*result = (*Node)(self).Owned()
	return nil
}
func (self *JSONApi) Describe(x Nothing, result *common.DHashDescription) error {
	*result = (*Node)(self).Description()
	return nil
}
func (self *JSONApi) DescribeTree(x Nothing, result *string) error {
	*result = (*Node)(self).DescribeTree()
	return nil
}
func (self *JSONApi) PrevIndex(i SubIndex, result *common.Item) error {
	data := common.Item{
		Key:   i.Key,
		Index: i.Index,
	}
	if f, e := self.forwardUnlessMe("DHash.PrevIndex", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).PrevIndex(data, result)
}
func (self *JSONApi) MirrorPrevIndex(i SubIndex, result *common.Item) error {
	data := common.Item{
		Key:   i.Key,
		Index: i.Index,
	}
	if f, e := self.forwardUnlessMe("DHash.MirrorPrevIndex", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).MirrorPrevIndex(data, result)
}
func (self *JSONApi) MirrorNextIndex(i SubIndex, result *common.Item) error {
	data := common.Item{
		Key:   i.Key,
		Index: i.Index,
	}
	if f, e := self.forwardUnlessMe("DHash.MirrorNextIndex", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).MirrorNextIndex(data, result)
}
func (self *JSONApi) NextIndex(i SubIndex, result *common.Item) error {
	data := common.Item{
		Key:   i.Key,
		Index: i.Index,
	}
	if f, e := self.forwardUnlessMe("DHash.NextIndex", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).NextIndex(data, result)
}
func (self *JSONApi) MirrorReverseIndexOf(i SubKeyReq, result *common.Index) error {
	data := common.Item{
		Key:    i.Key,
		SubKey: i.SubKey,
	}
	if f, e := self.forwardUnlessMe("DHash.MirrorReverseIndexOf", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).MirrorReverseIndexOf(data, result)
}
func (self *JSONApi) MirrorIndexOf(i SubKeyReq, result *common.Index) error {
	data := common.Item{
		Key:    i.Key,
		SubKey: i.SubKey,
	}
	if f, e := self.forwardUnlessMe("DHash.MirrorIndexOf", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).MirrorIndexOf(data, result)
}
func (self *JSONApi) ReverseIndexOf(i SubKeyReq, result *common.Index) error {
	data := common.Item{
		Key:    i.Key,
		SubKey: i.SubKey,
	}
	if f, e := self.forwardUnlessMe("DHash.ReverseIndexOf", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).ReverseIndexOf(data, result)
}
func (self *JSONApi) IndexOf(i SubKeyReq, result *common.Index) error {
	data := common.Item{
		Key:    i.Key,
		SubKey: i.SubKey,
	}
	if f, e := self.forwardUnlessMe("DHash.IndexOf", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).IndexOf(data, result)
}
func (self *JSONApi) SubMirrorPrev(k SubKeyReq, result *common.Item) error {
	data := common.Item{
		Key:    k.Key,
		SubKey: k.SubKey,
	}
	if f, e := self.forwardUnlessMe("DHash.SubMirrorPrev", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).SubMirrorPrev(data, result)
}
func (self *JSONApi) SubMirrorNext(k SubKeyReq, result *common.Item) error {
	data := common.Item{
		Key:    k.Key,
		SubKey: k.SubKey,
	}
	if f, e := self.forwardUnlessMe("DHash.SubMirrorNext", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).SubMirrorNext(data, result)
}
func (self *JSONApi) SubPrev(k SubKeyReq, result *common.Item) error {
	data := common.Item{
		Key:    k.Key,
		SubKey: k.SubKey,
	}
	if f, e := self.forwardUnlessMe("DHash.SubPrev", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).SubPrev(data, result)
}
func (self *JSONApi) SubNext(k SubKeyReq, result *common.Item) error {
	data := common.Item{
		Key:    k.Key,
		SubKey: k.SubKey,
	}
	if f, e := self.forwardUnlessMe("DHash.SubNext", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).SubNext(data, result)
}
func (self *JSONApi) MirrorFirst(k KeyReq, result *common.Item) error {
	data := common.Item{
		Key: k.Key,
	}
	if f, e := self.forwardUnlessMe("DHash.MirrorFirst", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).MirrorFirst(data, result)
}
func (self *JSONApi) MirrorLast(k KeyReq, result *common.Item) error {
	data := common.Item{
		Key: k.Key,
	}
	if f, e := self.forwardUnlessMe("DHash.MirrorLast", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).MirrorLast(data, result)
}
func (self *JSONApi) First(k KeyReq, result *common.Item) error {
	data := common.Item{
		Key: k.Key,
	}
	if f, e := self.forwardUnlessMe("DHash.First", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).First(data, result)
}
func (self *JSONApi) Last(k KeyReq, result *common.Item) error {
	data := common.Item{
		Key: k.Key,
	}
	if f, e := self.forwardUnlessMe("DHash.Last", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).Last(data, result)
}
func (self *JSONApi) MirrorReverseSlice(kr KeyRange, result *[]common.Item) error {
	r := common.Range{
		Key:    kr.Key,
		Min:    kr.Min,
		Max:    kr.Max,
		MinInc: kr.MinInc,
		MaxInc: kr.MaxInc,
	}
	if f, e := self.forwardUnlessMe("DHash.MirrorReverseSlice", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).MirrorReverseSlice(r, result)
}
func (self *JSONApi) MirrorSlice(kr KeyRange, result *[]common.Item) error {
	r := common.Range{
		Key:    kr.Key,
		Min:    kr.Min,
		Max:    kr.Max,
		MinInc: kr.MinInc,
		MaxInc: kr.MaxInc,
	}
	if f, e := self.forwardUnlessMe("DHash.MirrorSlice", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).MirrorSlice(r, result)
}
func (self *JSONApi) MirrorSliceIndex(ir IndexRange, result *[]common.Item) error {
	var mi int
	var ma int
	if ir.MinIndex != nil {
		mi = *ir.MinIndex
	}
	if ir.MaxIndex != nil {
		ma = *ir.MaxIndex
	}
	r := common.Range{
		Key:      ir.Key,
		MinIndex: mi,
		MaxIndex: ma,
		MinInc:   ir.MinIndex != nil,
		MaxInc:   ir.MaxIndex != nil,
	}
	if f, e := self.forwardUnlessMe("DHash.MirrorSliceIndex", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).MirrorSliceIndex(r, result)
}
func (self *JSONApi) MirrorReverseSliceIndex(ir IndexRange, result *[]common.Item) error {
	var mi int
	var ma int
	if ir.MinIndex != nil {
		mi = *ir.MinIndex
	}
	if ir.MaxIndex != nil {
		ma = *ir.MaxIndex
	}
	r := common.Range{
		Key:      ir.Key,
		MinIndex: mi,
		MaxIndex: ma,
		MinInc:   ir.MinIndex != nil,
		MaxInc:   ir.MaxIndex != nil,
	}
	if f, e := self.forwardUnlessMe("DHash.MirrorReverseSliceIndex", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).MirrorReverseSliceIndex(r, result)
}
func (self *JSONApi) MirrorSliceLen(pr PageRange, result *[]common.Item) error {
	r := common.Range{
		Key:    pr.Key,
		Min:    pr.From,
		MinInc: pr.FromInc,
		Len:    pr.Len,
	}
	if f, e := self.forwardUnlessMe("DHash.MirrorSliceLen", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).MirrorSliceLen(r, result)
}
func (self *JSONApi) MirrorReverseSliceLen(pr PageRange, result *[]common.Item) error {
	r := common.Range{
		Key:    pr.Key,
		Max:    pr.From,
		MaxInc: pr.FromInc,
		Len:    pr.Len,
	}
	if f, e := self.forwardUnlessMe("DHash.MirrorReverseSliceLen", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).MirrorReverseSliceLen(r, result)
}
func (self *JSONApi) ReverseSlice(kr KeyRange, result *[]common.Item) error {
	r := common.Range{
		Key:    kr.Key,
		Min:    kr.Min,
		Max:    kr.Max,
		MinInc: kr.MinInc,
		MaxInc: kr.MaxInc,
	}
	if f, e := self.forwardUnlessMe("DHash.ReverseSlice", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).ReverseSlice(r, result)
}
func (self *JSONApi) Slice(kr KeyRange, result *[]common.Item) error {
	r := common.Range{
		Key:    kr.Key,
		Min:    kr.Min,
		Max:    kr.Max,
		MinInc: kr.MinInc,
		MaxInc: kr.MaxInc,
	}
	if f, e := self.forwardUnlessMe("DHash.Slice", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).Slice(r, result)
}
func (self *JSONApi) SliceIndex(ir IndexRange, result *[]common.Item) error {
	var mi int
	var ma int
	if ir.MinIndex != nil {
		mi = *ir.MinIndex
	}
	if ir.MaxIndex != nil {
		ma = *ir.MaxIndex
	}
	r := common.Range{
		Key:      ir.Key,
		MinIndex: mi,
		MaxIndex: ma,
		MinInc:   ir.MinIndex != nil,
		MaxInc:   ir.MaxIndex != nil,
	}
	if f, e := self.forwardUnlessMe("DHash.SliceIndex", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).SliceIndex(r, result)
}
func (self *JSONApi) ReverseSliceIndex(ir IndexRange, result *[]common.Item) error {
	var mi int
	var ma int
	if ir.MinIndex != nil {
		mi = *ir.MinIndex
	}
	if ir.MaxIndex != nil {
		ma = *ir.MaxIndex
	}
	r := common.Range{
		Key:      ir.Key,
		MinIndex: mi,
		MaxIndex: ma,
		MinInc:   ir.MinIndex != nil,
		MaxInc:   ir.MaxIndex != nil,
	}
	if f, e := self.forwardUnlessMe("DHash.ReverseSliceIndex", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).ReverseSliceIndex(r, result)
}
func (self *JSONApi) SliceLen(pr PageRange, result *[]common.Item) error {
	r := common.Range{
		Key:    pr.Key,
		Min:    pr.From,
		MinInc: pr.FromInc,
		Len:    pr.Len,
	}
	if f, e := self.forwardUnlessMe("DHash.SliceLen", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).SliceLen(r, result)
}
func (self *JSONApi) ReverseSliceLen(pr PageRange, result *[]common.Item) error {
	r := common.Range{
		Key:    pr.Key,
		Max:    pr.From,
		MaxInc: pr.FromInc,
		Len:    pr.Len,
	}
	if f, e := self.forwardUnlessMe("DHash.ReverseSliceLen", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).ReverseSliceLen(r, result)
}
func (self *JSONApi) SetExpression(expr setop.SetExpression, items *[]setop.SetOpResult) error {
	if expr.Op == nil {
		var err error
		if expr.Op, err = setop.NewSetOpParser(expr.Code).Parse(); err != nil {
			return err
		}
	}
	return (*Node)(self).SetExpression(expr, items)
}

func (self *JSONApi) AddConfiguration(co Conf, x *Nothing) error {
	c := common.ConfItem{
		Key:   co.Key,
		Value: co.Value,
	}
	(*Node)(self).AddConfiguration(c)
	return nil
}
func (self *JSONApi) SubAddConfiguration(co SubConf, x *Nothing) error {
	c := common.ConfItem{
		TreeKey: co.TreeKey,
		Key:     co.Key,
		Value:   co.Value,
	}
	(*Node)(self).SubAddConfiguration(c)
	return nil
}
func (self *JSONApi) Configuration(x Nothing, result *common.Conf) error {
	*result = common.Conf{}
	(*result).Data, (*result).Timestamp = (*Node)(self).tree.Configuration()
	return nil
}
func (self *JSONApi) SubConfiguration(key []byte, result *common.Conf) error {
	*result = common.Conf{TreeKey: key}
	(*result).Data, (*result).Timestamp = (*Node)(self).tree.SubConfiguration(key)
	return nil
}

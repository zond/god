package dhash

import (
	"github.com/zond/god/common"
	"github.com/zond/god/setop"
)

type jsonDhashServer Node

func (self *jsonDhashServer) forwardUnlessMe(cmd string, key []byte, in, out interface{}) (forwarded bool, err error) {
	succ := (*Node)(self).node.GetSuccessorFor(key)
	if succ.Addr != (*Node)(self).node.GetAddr() {
		forwarded, err = true, succ.Call(cmd, in, out)
	}
	return
}

func (self *jsonDhashServer) Kill(x int, y *int) error {
	(*Node)(self).Kill()
	return nil
}
func (self *jsonDhashServer) Clear(x int, y *int) error {
	(*Node)(self).Clear()
	return nil
}
func (self *jsonDhashServer) SlaveSubPut(data common.Item, x *int) error {
	return (*Node)(self).subPut(data)
}
func (self *jsonDhashServer) SlaveSubClear(data common.Item, x *int) error {
	return (*Node)(self).subClear(data)
}
func (self *jsonDhashServer) SlaveSubDel(data common.Item, x *int) error {
	return (*Node)(self).subDel(data)
}
func (self *jsonDhashServer) SlaveDel(data common.Item, x *int) error {
	return (*Node)(self).del(data)
}
func (self *jsonDhashServer) SlavePut(data common.Item, x *int) error {
	return (*Node)(self).put(data)
}
func (self *jsonDhashServer) SubDel(data common.Item, x *int) error {
	if f, e := self.forwardUnlessMe("DHash.SubDel", data.Key, data, x); f {
		return e
	}
	return (*Node)(self).SubDel(data)
}
func (self *jsonDhashServer) SubClear(data common.Item, x *int) error {
	if f, e := self.forwardUnlessMe("DHash.SubClear", data.Key, data, x); f {
		return e
	}
	return (*Node)(self).SubClear(data)
}
func (self *jsonDhashServer) SubPut(data common.Item, x *int) error {
	if f, e := self.forwardUnlessMe("DHash.SubPut", data.Key, data, x); f {
		return e
	}
	return (*Node)(self).SubPut(data)
}
func (self *jsonDhashServer) Del(data common.Item, x *int) error {
	if f, e := self.forwardUnlessMe("DHash.Del", data.Key, data, x); f {
		return e
	}
	return (*Node)(self).Del(data)
}
func (self *jsonDhashServer) Put(data common.Item, x *int) error {
	if f, e := self.forwardUnlessMe("DHash.Put", data.Key, data, x); f {
		return e
	}
	return (*Node)(self).Put(data)
}
func (self *jsonDhashServer) RingHash(x int, result *[]byte) error {
	return (*Node)(self).RingHash(x, result)
}
func (self *jsonDhashServer) MirrorCount(r common.Range, result *int) error {
	if f, e := self.forwardUnlessMe("DHash.MirrorCount", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).MirrorCount(r, result)
}
func (self *jsonDhashServer) Count(r common.Range, result *int) error {
	if f, e := self.forwardUnlessMe("DHash.Count", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).Count(r, result)
}
func (self *jsonDhashServer) Next(data common.Item, result *common.Item) error {
	return (*Node)(self).Next(data, result)
}
func (self *jsonDhashServer) Prev(data common.Item, result *common.Item) error {
	return (*Node)(self).Prev(data, result)
}
func (self *jsonDhashServer) SubGet(data common.Item, result *common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.SubGet", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).SubGet(data, result)
}
func (self *jsonDhashServer) Get(data common.Item, result *common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.Get", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).Get(data, result)
}
func (self *jsonDhashServer) Size(x int, result *int) error {
	*result = (*Node)(self).Size()
	return nil
}
func (self *jsonDhashServer) SubSize(key []byte, result *int) error {
	if f, e := self.forwardUnlessMe("DHash.SubSize", key, key, result); f {
		return e
	}
	return (*Node)(self).SubSize(key, result)
}
func (self *jsonDhashServer) Owned(x int, result *int) error {
	*result = (*Node)(self).Owned()
	return nil
}
func (self *jsonDhashServer) Describe(x int, result *common.DHashDescription) error {
	*result = (*Node)(self).Description()
	return nil
}
func (self *jsonDhashServer) DescribeTree(x int, result *string) error {
	*result = (*Node)(self).DescribeTree()
	return nil
}
func (self *jsonDhashServer) PrevIndex(data common.Item, result *common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.PrevIndex", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).PrevIndex(data, result)
}
func (self *jsonDhashServer) MirrorPrevIndex(data common.Item, result *common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.MirrorPrevIndex", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).MirrorPrevIndex(data, result)
}
func (self *jsonDhashServer) MirrorNextIndex(data common.Item, result *common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.MirrorNextIndex", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).MirrorNextIndex(data, result)
}
func (self *jsonDhashServer) NextIndex(data common.Item, result *common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.NextIndex", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).NextIndex(data, result)
}
func (self *jsonDhashServer) MirrorReverseIndexOf(data common.Item, result *common.Index) error {
	if f, e := self.forwardUnlessMe("DHash.MirrorReverseIndexOf", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).MirrorReverseIndexOf(data, result)
}
func (self *jsonDhashServer) MirrorIndexOf(data common.Item, result *common.Index) error {
	if f, e := self.forwardUnlessMe("DHash.MirrorIndexOf", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).MirrorIndexOf(data, result)
}
func (self *jsonDhashServer) ReverseIndexOf(data common.Item, result *common.Index) error {
	if f, e := self.forwardUnlessMe("DHash.ReverseIndexOf", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).ReverseIndexOf(data, result)
}
func (self *jsonDhashServer) IndexOf(data common.Item, result *common.Index) error {
	if f, e := self.forwardUnlessMe("DHash.IndexOf", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).IndexOf(data, result)
}
func (self *jsonDhashServer) SubMirrorPrev(data common.Item, result *common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.SubMirrorPrev", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).SubMirrorPrev(data, result)
}
func (self *jsonDhashServer) SubMirrorNext(data common.Item, result *common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.SubMirrorNext", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).SubMirrorNext(data, result)
}
func (self *jsonDhashServer) SubPrev(data common.Item, result *common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.SubPrev", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).SubPrev(data, result)
}
func (self *jsonDhashServer) SubNext(data common.Item, result *common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.SubNext", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).SubNext(data, result)
}
func (self *jsonDhashServer) MirrorFirst(data common.Item, result *common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.MirrorFirst", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).MirrorFirst(data, result)
}
func (self *jsonDhashServer) MirrorLast(data common.Item, result *common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.MirrorLast", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).MirrorLast(data, result)
}
func (self *jsonDhashServer) First(data common.Item, result *common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.First", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).First(data, result)
}
func (self *jsonDhashServer) Last(data common.Item, result *common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.Last", data.Key, data, result); f {
		return e
	}
	return (*Node)(self).Last(data, result)
}
func (self *jsonDhashServer) MirrorReverseSlice(r common.Range, result *[]common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.MirrorReverseSlice", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).MirrorReverseSlice(r, result)
}
func (self *jsonDhashServer) MirrorSlice(r common.Range, result *[]common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.MirrorSlice", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).MirrorSlice(r, result)
}
func (self *jsonDhashServer) MirrorSliceIndex(r common.Range, result *[]common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.MirrorSliceIndex", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).MirrorSliceIndex(r, result)
}
func (self *jsonDhashServer) MirrorReverseSliceIndex(r common.Range, result *[]common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.MirrorReverseSliceIndex", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).MirrorReverseSliceIndex(r, result)
}
func (self *jsonDhashServer) MirrorSliceLen(r common.Range, result *[]common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.MirrorSliceLen", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).MirrorSliceLen(r, result)
}
func (self *jsonDhashServer) MirrorReverseSliceLen(r common.Range, result *[]common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.MirrorReverseSliceLen", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).MirrorSliceLen(r, result)
}
func (self *jsonDhashServer) ReverseSlice(r common.Range, result *[]common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.ReverseSlice", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).ReverseSlice(r, result)
}
func (self *jsonDhashServer) Slice(r common.Range, result *[]common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.Slice", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).Slice(r, result)
}
func (self *jsonDhashServer) SliceIndex(r common.Range, result *[]common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.SliceIndex", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).SliceIndex(r, result)
}
func (self *jsonDhashServer) ReverseSliceIndex(r common.Range, result *[]common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.ReverseSliceIndex", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).ReverseSliceIndex(r, result)
}
func (self *jsonDhashServer) SliceLen(r common.Range, result *[]common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.SliceLen", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).SliceLen(r, result)
}
func (self *jsonDhashServer) ReverseSliceLen(r common.Range, result *[]common.Item) error {
	if f, e := self.forwardUnlessMe("DHash.ReverseSliceLen", r.Key, r, result); f {
		return e
	}
	return (*Node)(self).SliceLen(r, result)
}
func (self *jsonDhashServer) SetExpression(expr setop.SetExpression, items *[]setop.SetOpResult) error {
	return (*Node)(self).SetExpression(expr, items)
}

func (self *jsonDhashServer) AddConfiguration(c common.ConfItem, x *int) error {
	(*Node)(self).AddConfiguration(c)
	return nil
}
func (self *jsonDhashServer) SubAddConfiguration(c common.ConfItem, x *int) error {
	(*Node)(self).SubAddConfiguration(c)
	return nil
}
func (self *jsonDhashServer) Configuration(x int, result *common.Conf) error {
	*result = common.Conf{}
	(*result).Data, (*result).Timestamp = (*Node)(self).tree.Configuration()
	return nil
}
func (self *jsonDhashServer) SubConfiguration(key []byte, result *common.Conf) error {
	*result = common.Conf{TreeKey: key}
	(*result).Data, (*result).Timestamp = (*Node)(self).tree.SubConfiguration(key)
	return nil
}

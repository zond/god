package dhash

import (
	"../common"
)

type dhashServer DHash

func (self *dhashServer) SlaveSubPut(data common.Item, x *int) error {
	return (*DHash)(self).subPut(data)
}
func (self *dhashServer) SubPut(data common.Item, x *int) error {
	return (*DHash)(self).SubPut(data)
}
func (self *dhashServer) SlavePut(data common.Item, x *int) error {
	return (*DHash)(self).put(data)
}
func (self *dhashServer) Put(data common.Item, x *int) error {
	return (*DHash)(self).Put(data)
}
func (self *dhashServer) RingHash(x int, result *[]byte) error {
	return (*DHash)(self).RingHash(x, result)
}
func (self *dhashServer) SubFind(data common.Item, result *common.Item) error {
	return (*DHash)(self).SubFind(data, result)
}
func (self *dhashServer) Find(data common.Item, result *common.Item) error {
	return (*DHash)(self).Find(data, result)
}
func (self *dhashServer) Count(r common.Range, result *int) error {
	return (*DHash)(self).Count(r, result)
}
func (self *dhashServer) Next(data common.Item, result *common.Item) error {
	return (*DHash)(self).Next(data, result)
}
func (self *dhashServer) Prev(data common.Item, result *common.Item) error {
	return (*DHash)(self).Prev(data, result)
}
func (self *dhashServer) SubGet(data common.Item, result *common.Item) error {
	return (*DHash)(self).SubGet(data, result)
}
func (self *dhashServer) Get(data common.Item, result *common.Item) error {
	return (*DHash)(self).Get(data, result)
}
func (self *dhashServer) DescribeTree(x int, result *string) error {
	*result = (*DHash)(self).DescribeTree()
	return nil
}
func (self *dhashServer) PrevIndex(data common.Item, result *common.Item) error {
	return (*DHash)(self).PrevIndex(data, result)
}
func (self *dhashServer) NextIndex(data common.Item, result *common.Item) error {
	return (*DHash)(self).NextIndex(data, result)
}
func (self *dhashServer) ReverseIndexOf(data common.Item, result *common.Index) error {
	return (*DHash)(self).ReverseIndexOf(data, result)
}
func (self *dhashServer) IndexOf(data common.Item, result *common.Index) error {
	return (*DHash)(self).IndexOf(data, result)
}
func (self *dhashServer) SubPrev(data common.Item, result *common.Item) error {
	return (*DHash)(self).SubPrev(data, result)
}
func (self *dhashServer) SubNext(data common.Item, result *common.Item) error {
	return (*DHash)(self).SubNext(data, result)
}
func (self *dhashServer) First(data common.Item, result *common.Item) error {
	return (*DHash)(self).First(data, result)
}
func (self *dhashServer) Last(data common.Item, result *common.Item) error {
	return (*DHash)(self).Last(data, result)
}
func (self *dhashServer) ReverseSlice(r common.Range, result *[]common.Item) error {
	return (*DHash)(self).ReverseSlice(r, result)
}
func (self *dhashServer) Slice(r common.Range, result *[]common.Item) error {
	return (*DHash)(self).Slice(r, result)
}
func (self *dhashServer) SliceIndex(r common.Range, result *[]common.Item) error {
	return (*DHash)(self).SliceIndex(r, result)
}
func (self *dhashServer) ReverseSliceIndex(r common.Range, result *[]common.Item) error {
	return (*DHash)(self).ReverseSliceIndex(r, result)
}

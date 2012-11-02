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
func (self *dhashServer) Find(data common.Item, result *common.Item) error {
	return (*DHash)(self).Find(data, result)
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

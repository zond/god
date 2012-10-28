package dhash

import (
	"../common"
)

type dhashServer DHash

func (self *dhashServer) SlavePut(data common.Item, x *int) error {
	return (*DHash)(self).put(data)
}
func (self *dhashServer) Put(data common.Item, x *int) error {
	return (*DHash)(self).Put(data)
}
func (self *dhashServer) Find(data common.Item, result *common.Item) error {
	return (*DHash)(self).Find(data, result)
}
func (self *dhashServer) Get(data common.Item, result *common.Item) error {
	return (*DHash)(self).Get(data, result)
}
func (self *dhashServer) DescribeTree(x int, result *string) error {
	*result = (*DHash)(self).DescribeTree()
	return nil
}

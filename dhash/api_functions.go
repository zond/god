package dhash

import (
	"../client"
	"../common"
	"../radix"
	"bytes"
	"fmt"
	"sync/atomic"
	"time"
)

func (self *Node) Description() common.DHashDescription {
	return common.DHashDescription{
		Addr:         self.GetAddr(),
		Pos:          self.node.GetPosition(),
		LastReroute:  time.Unix(0, atomic.LoadInt64(&self.lastReroute)),
		LastSync:     time.Unix(0, atomic.LoadInt64(&self.lastSync)),
		LastMigrate:  time.Unix(0, atomic.LoadInt64(&self.lastMigrate)),
		Timer:        self.timer.ActualTime(),
		OwnedEntries: self.Owned(),
		HeldEntries:  self.tree.RealSize(),
		Nodes:        self.node.GetNodes(),
	}
}
func (self *Node) Describe() string {
	return self.Description().Describe()
}
func (self *Node) DescribeTree() string {
	return self.tree.Describe()
}
func (self *Node) client() *client.Conn {
	return client.NewConnRing(common.NewRingNodes(self.node.Nodes()))
}
func (self *Node) SubFind(data common.Item, result *common.Item) error {
	*result = data
	result.Value, result.Exists = self.client().Get(data.Key)
	return nil
}
func (self *Node) Find(data common.Item, result *common.Item) error {
	*result = data
	result.Value, result.Exists = self.client().Get(data.Key)
	return nil
}
func (self *Node) Get(data common.Item, result *common.Item) error {
	*result = data
	result.Value, result.Timestamp, result.Exists = self.tree.Get(data.Key)
	return nil
}
func (self *Node) Prev(data common.Item, result *common.Item) error {
	*result = data
	result.Key, result.Value, result.Timestamp, result.Exists = self.tree.Prev(data.Key)
	return nil
}
func (self *Node) Next(data common.Item, result *common.Item) error {
	*result = data
	result.Key, result.Value, result.Timestamp, result.Exists = self.tree.Next(data.Key)
	return nil
}
func (self *Node) RingHash(x int, ringHash *[]byte) error {
	*ringHash = self.node.RingHash()
	return nil
}
func (self *Node) MirrorCount(r common.Range, result *int) error {
	*result = self.tree.SubMirrorSizeBetween(r.Key, r.Min, r.Max, r.MinInc, r.MaxInc)
	return nil
}
func (self *Node) Count(r common.Range, result *int) error {
	*result = self.tree.SubSizeBetween(r.Key, r.Min, r.Max, r.MinInc, r.MaxInc)
	return nil
}
func (self *Node) MirrorLast(data common.Item, result *common.Item) error {
	result.Key, result.Value, result.Timestamp, result.Exists = self.tree.SubMirrorLast(data.Key)
	return nil
}
func (self *Node) MirrorFirst(data common.Item, result *common.Item) error {
	result.Key, result.Value, result.Timestamp, result.Exists = self.tree.SubMirrorFirst(data.Key)
	return nil
}
func (self *Node) Last(data common.Item, result *common.Item) error {
	result.Key, result.Value, result.Timestamp, result.Exists = self.tree.SubLast(data.Key)
	return nil
}
func (self *Node) First(data common.Item, result *common.Item) error {
	result.Key, result.Value, result.Timestamp, result.Exists = self.tree.SubFirst(data.Key)
	return nil
}
func (self *Node) MirrorPrevIndex(data common.Item, result *common.Item) error {
	result.Key, result.Value, result.Timestamp, result.Index, result.Exists = self.tree.SubMirrorPrevIndex(data.Key, data.Index)
	return nil
}
func (self *Node) MirrorNextIndex(data common.Item, result *common.Item) error {
	result.Key, result.Value, result.Timestamp, result.Index, result.Exists = self.tree.SubMirrorNextIndex(data.Key, data.Index)
	return nil
}
func (self *Node) PrevIndex(data common.Item, result *common.Item) error {
	result.Key, result.Value, result.Timestamp, result.Index, result.Exists = self.tree.SubPrevIndex(data.Key, data.Index)
	return nil
}
func (self *Node) NextIndex(data common.Item, result *common.Item) error {
	result.Key, result.Value, result.Timestamp, result.Index, result.Exists = self.tree.SubNextIndex(data.Key, data.Index)
	return nil
}
func (self *Node) SubMirrorPrev(data common.Item, result *common.Item) error {
	result.Key, result.Value, result.Timestamp, result.Exists = self.tree.SubMirrorPrev(data.Key, data.SubKey)
	return nil
}
func (self *Node) SubMirrorNext(data common.Item, result *common.Item) error {
	result.Key, result.Value, result.Timestamp, result.Exists = self.tree.SubMirrorNext(data.Key, data.SubKey)
	return nil
}
func (self *Node) SubPrev(data common.Item, result *common.Item) error {
	result.Key, result.Value, result.Timestamp, result.Exists = self.tree.SubPrev(data.Key, data.SubKey)
	return nil
}
func (self *Node) SubNext(data common.Item, result *common.Item) error {
	result.Key, result.Value, result.Timestamp, result.Exists = self.tree.SubNext(data.Key, data.SubKey)
	return nil
}
func (self *Node) SliceIndex(r common.Range, items *[]common.Item) error {
	min := &r.MinIndex
	max := &r.MaxIndex
	if !r.MinInc {
		min = nil
	}
	if !r.MaxInc {
		max = nil
	}
	self.tree.SubEachBetweenIndex(r.Key, min, max, func(key []byte, value []byte, version int64, index int) bool {
		*items = append(*items, common.Item{
			Key:       key,
			Value:     value,
			Timestamp: version,
			Index:     index,
		})
		return true
	})
	return nil
}
func (self *Node) ReverseSliceIndex(r common.Range, items *[]common.Item) error {
	min := &r.MinIndex
	max := &r.MaxIndex
	if !r.MinInc {
		min = nil
	}
	if !r.MaxInc {
		max = nil
	}
	self.tree.SubReverseEachBetweenIndex(r.Key, min, max, func(key []byte, value []byte, version int64, index int) bool {
		*items = append(*items, common.Item{
			Key:       key,
			Value:     value,
			Timestamp: version,
			Index:     index,
		})
		return true
	})
	return nil
}
func (self *Node) ReverseSlice(r common.Range, items *[]common.Item) error {
	self.tree.SubReverseEachBetween(r.Key, r.Min, r.Max, r.MinInc, r.MaxInc, func(key []byte, value []byte, version int64) bool {
		*items = append(*items, common.Item{
			Key:       key,
			Value:     value,
			Timestamp: version,
		})
		return true
	})
	return nil
}
func (self *Node) Slice(r common.Range, items *[]common.Item) error {
	self.tree.SubEachBetween(r.Key, r.Min, r.Max, r.MinInc, r.MaxInc, func(key []byte, value []byte, version int64) bool {
		*items = append(*items, common.Item{
			Key:       key,
			Value:     value,
			Timestamp: version,
		})
		return true
	})
	return nil
}
func (self *Node) SliceLen(r common.Range, items *[]common.Item) error {
	self.tree.SubEachBetween(r.Key, r.Min, nil, r.MinInc, false, func(key []byte, value []byte, version int64) bool {
		*items = append(*items, common.Item{
			Key:       key,
			Value:     value,
			Timestamp: version,
		})
		return len(*items) < r.Len
	})
	return nil
}
func (self *Node) ReverseSliceLen(r common.Range, items *[]common.Item) error {
	self.tree.SubReverseEachBetween(r.Key, nil, r.Max, false, r.MaxInc, func(key []byte, value []byte, version int64) bool {
		*items = append(*items, common.Item{
			Key:       key,
			Value:     value,
			Timestamp: version,
		})
		return len(*items) < r.Len
	})
	return nil
}
func (self *Node) MirrorSliceIndex(r common.Range, items *[]common.Item) error {
	min := &r.MinIndex
	max := &r.MaxIndex
	if !r.MinInc {
		min = nil
	}
	if !r.MaxInc {
		max = nil
	}
	self.tree.SubMirrorEachBetweenIndex(r.Key, min, max, func(key []byte, value []byte, version int64, index int) bool {
		*items = append(*items, common.Item{
			Key:       key,
			Value:     value,
			Timestamp: version,
			Index:     index,
		})
		return true
	})
	return nil
}
func (self *Node) MirrorReverseSliceIndex(r common.Range, items *[]common.Item) error {
	min := &r.MinIndex
	max := &r.MaxIndex
	if !r.MinInc {
		min = nil
	}
	if !r.MaxInc {
		max = nil
	}
	self.tree.SubMirrorReverseEachBetweenIndex(r.Key, min, max, func(key []byte, value []byte, version int64, index int) bool {
		*items = append(*items, common.Item{
			Key:       key,
			Value:     value,
			Timestamp: version,
			Index:     index,
		})
		return true
	})
	return nil
}
func (self *Node) MirrorReverseSlice(r common.Range, items *[]common.Item) error {
	self.tree.SubMirrorReverseEachBetween(r.Key, r.Min, r.Max, r.MinInc, r.MaxInc, func(key []byte, value []byte, version int64) bool {
		*items = append(*items, common.Item{
			Key:       key,
			Value:     value,
			Timestamp: version,
		})
		return true
	})
	return nil
}
func (self *Node) MirrorSlice(r common.Range, items *[]common.Item) error {
	self.tree.SubMirrorEachBetween(r.Key, r.Min, r.Max, r.MinInc, r.MaxInc, func(key []byte, value []byte, version int64) bool {
		*items = append(*items, common.Item{
			Key:       key,
			Value:     value,
			Timestamp: version,
		})
		return true
	})
	return nil
}
func (self *Node) MirrorSliceLen(r common.Range, items *[]common.Item) error {
	self.tree.SubMirrorEachBetween(r.Key, r.Min, nil, r.MinInc, false, func(key []byte, value []byte, version int64) bool {
		*items = append(*items, common.Item{
			Key:       key,
			Value:     value,
			Timestamp: version,
		})
		return len(*items) < r.Len
	})
	return nil
}
func (self *Node) MirrorReverseSliceLen(r common.Range, items *[]common.Item) error {
	self.tree.SubMirrorReverseEachBetween(r.Key, nil, r.Max, false, r.MaxInc, func(key []byte, value []byte, version int64) bool {
		*items = append(*items, common.Item{
			Key:       key,
			Value:     value,
			Timestamp: version,
		})
		return len(*items) < r.Len
	})
	return nil
}
func (self *Node) MirrorReverseIndexOf(data common.Item, result *common.Index) error {
	result.N, result.Existed = self.tree.SubMirrorReverseIndexOf(data.Key, data.SubKey)
	return nil
}
func (self *Node) MirrorIndexOf(data common.Item, result *common.Index) error {
	result.N, result.Existed = self.tree.SubMirrorIndexOf(data.Key, data.SubKey)
	return nil
}
func (self *Node) ReverseIndexOf(data common.Item, result *common.Index) error {
	result.N, result.Existed = self.tree.SubReverseIndexOf(data.Key, data.SubKey)
	return nil
}
func (self *Node) IndexOf(data common.Item, result *common.Index) error {
	result.N, result.Existed = self.tree.SubIndexOf(data.Key, data.SubKey)
	return nil
}
func (self *Node) SubGet(data common.Item, result *common.Item) error {
	*result = data
	result.Value, result.Timestamp, result.Exists = self.tree.SubGet(data.Key, data.SubKey)
	return nil
}
func (self *Node) SubClear(data common.Item) error {
	successor := self.node.GetSuccessorFor(data.Key)
	if successor.Addr != self.node.GetAddr() {
		var x int
		return successor.Call("DHash.SubClear", data, &x)
	}
	data.TTL, data.Timestamp = self.node.Redundancy(), self.timer.ContinuousTime()
	return self.subClear(data)
}
func (self *Node) SubDel(data common.Item) error {
	successor := self.node.GetSuccessorFor(data.Key)
	if successor.Addr != self.node.GetAddr() {
		var x int
		return successor.Call("DHash.SubDel", data, &x)
	}
	data.TTL, data.Timestamp = self.node.Redundancy(), self.timer.ContinuousTime()
	return self.subDel(data)
}
func (self *Node) SubPut(data common.Item) error {
	successor := self.node.GetSuccessorFor(data.Key)
	if successor.Addr != self.node.GetAddr() {
		var x int
		return successor.Call("DHash.SubPut", data, &x)
	}
	data.TTL, data.Timestamp = self.node.Redundancy(), self.timer.ContinuousTime()
	return self.subPut(data)
}
func (self *Node) Del(data common.Item) error {
	successor := self.node.GetSuccessorFor(data.Key)
	if successor.Addr != self.node.GetAddr() {
		var x int
		return successor.Call("DHash.Del", data, &x)
	}
	data.TTL, data.Timestamp = self.node.Redundancy(), self.timer.ContinuousTime()
	return self.del(data)
}
func (self *Node) Put(data common.Item) error {
	successor := self.node.GetSuccessorFor(data.Key)
	if successor.Addr != self.node.GetAddr() {
		var x int
		return successor.Call("DHash.Put", data, &x)
	}
	data.TTL, data.Timestamp = self.node.Redundancy(), self.timer.ContinuousTime()
	return self.put(data)
}
func (self *Node) forwardOperation(data common.Item, operation string) {
	data.TTL--
	successor := self.node.GetSuccessor()
	var x int
	err := successor.Call(operation, data, &x)
	for err != nil {
		self.node.RemoveNode(successor)
		successor = self.node.GetSuccessor()
		err = successor.Call(operation, data, &x)
	}
}
func (self *Node) clear() {
	self.tree = radix.NewTreeTimer(self.timer)
}
func (self *Node) subClear(data common.Item) error {
	if data.TTL > 1 {
		if data.Sync {
			self.forwardOperation(data, "DHash.SlaveSubClear")
		} else {
			go self.forwardOperation(data, "DHash.SlaveSubClear")
		}
	}
	self.tree.SubClear(data.Key, data.Timestamp)
	return nil
}
func (self *Node) subDel(data common.Item) error {
	if data.TTL > 1 {
		if data.Sync {
			self.forwardOperation(data, "DHash.SlaveSubDel")
		} else {
			go self.forwardOperation(data, "DHash.SlaveSubDel")
		}
	}
	self.tree.SubFakeDel(data.Key, data.SubKey, data.Timestamp)
	return nil
}
func (self *Node) subPut(data common.Item) error {
	if data.TTL > 1 {
		if data.Sync {
			self.forwardOperation(data, "DHash.SlaveSubPut")
		} else {
			go self.forwardOperation(data, "DHash.SlaveSubPut")
		}
	}
	self.tree.SubPut(data.Key, data.SubKey, data.Value, data.Timestamp)
	return nil
}
func (self *Node) del(data common.Item) error {
	if data.TTL > 1 {
		if data.Sync {
			self.forwardOperation(data, "DHash.SlaveDel")
		} else {
			go self.forwardOperation(data, "DHash.SlaveDel")
		}
	}
	self.tree.FakeDel(data.Key, data.Timestamp)
	return nil
}
func (self *Node) put(data common.Item) error {
	if data.TTL > 1 {
		if data.Sync {
			self.forwardOperation(data, "DHash.SlavePut")
		} else {
			go self.forwardOperation(data, "DHash.SlavePut")
		}
	}
	self.tree.Put(data.Key, data.Value, data.Timestamp)
	return nil
}
func (self *Node) Size() int {
	pred := self.node.GetPredecessor()
	me := self.node.Remote()
	cmp := bytes.Compare(pred.Pos, me.Pos)
	if cmp < 0 {
		return self.tree.SizeBetween(pred.Pos, me.Pos, true, false)
	} else if cmp > 0 {
		return self.tree.SizeBetween(pred.Pos, nil, true, false) + self.tree.SizeBetween(nil, me.Pos, true, false)
	}
	if pred.Less(me) {
		return 0
	}
	return self.tree.Size()
}
func (self *Node) SubSize(key []byte, result *int) error {
	*result = self.tree.SubSize(key)
	return nil
}
func (self *Node) SetExpression(expr common.SetExpression, items *[]common.SetOpResult) (err error) {
	data := common.Item{
		Key: expr.Dest,
	}
	if expr.Dest != nil {
		if expr.Op.Merge == common.Append {
			err = fmt.Errorf("When storing results of Set expressions the Append merge function is not allowed")
			return
		}
		successor := self.node.GetSuccessorFor(expr.Dest)
		if successor.Addr != self.node.GetAddr() {
			return successor.Call("DHash.SetExpression", expr, items)
		}
	}
	skipper := self.createSkipper(expr.Op)
	var res *common.SetOpResult
	for res, err = skipper.skip(expr.Min, expr.MinInc); res != nil && err == nil; res, err = skipper.skip(expr.Min, expr.MinInc) {
		expr.Min = res.Key
		expr.MinInc = false
		if expr.Dest == nil {
			*items = append(*items, *res)
		} else {
			data.SubKey = res.Key
			data.Value = res.Values[0]
			data.TTL = self.node.Redundancy()
			data.Timestamp = self.timer.ContinuousTime()
			self.put(data)
		}
	}
	return
}
func (self *Node) AddConfiguration(c common.ConfItem) {
	self.tree.AddConfiguration(c.Key, c.Value)
}
func (self *Node) SubAddConfiguration(c common.ConfItem) {
	self.tree.SubAddConfiguration(c.TreeKey, c.Key, c.Value)
}

package dhash

import (
	"../common"
	"../radix"
	"bytes"
	"fmt"
)

const (
	bufferSize = 128
)

func (self *Node) createSkippers(sources []interface{}) (result []skipper) {
	result = make([]skipper, len(sources))
	for index, source := range sources {
		if op, ok := source.(common.SetOp); ok {
			result[index] = self.createSkipper(op)
		} else if key, ok := source.([]byte); ok {
			remote := self.node.GetSuccessorFor(key)
			var tree *radix.Tree
			if remote.Addr == self.node.GetAddr() {
				tree = self.tree
			}
			result[index] = &treeSkipper{
				key:    key,
				tree:   tree,
				remote: remote,
			}
		} else {
			panic(fmt.Errorf("Unknown SetOp Source type: %+v", source))
		}
	}
	return
}

func (self *Node) createSkipper(op common.SetOp) (result skipper) {
	switch op.Type {
	case common.Union:
		result = &unionOp{skippers: self.createSkippers(op.Sources)}
	case common.Intersection:
		result = &interOp{skippers: self.createSkippers(op.Sources)}
	case common.Difference:
		result = &diffOp{skippers: self.createSkippers(op.Sources)}
	default:
		panic(fmt.Errorf("Unknown SetOp Type %v", op.Type))
	}
	return
}

type skipper interface {
	// skip returns a value matching the min and inclusive criteria.
	// If the last yielded value matches the criteria the same value will be returned again.
	skip(min []byte, inc bool) (result *common.SetOpResult, err error)
}

type unionOp struct {
	skippers []skipper
	curr     *common.SetOpResult
}

func (self *unionOp) skip(min []byte, inc bool) (result *common.SetOpResult, err error) {
	gt := 0
	if inc {
		gt = -1
	}

	if self.curr != nil && bytes.Compare(self.curr.Key, min) > gt {
		result = self.curr
		return
	}

	newSkippers := make([]skipper, 0, len(self.skippers))

	var cmp int
	var res *common.SetOpResult
	for _, thisSkipper := range self.skippers {
		if res, err = thisSkipper.skip(min, inc); err != nil {
			result = nil
			self.curr = nil
			return
		}
		if res != nil {
			newSkippers = append(newSkippers, thisSkipper)
			if result == nil {
				result = res.ShallowCopy()
			} else {
				cmp = bytes.Compare(res.Key, result.Key)
				if cmp < 0 {
					result = res.ShallowCopy()
				} else if cmp == 0 {
					result.Values = append(result.Values, res.Values...)
				}
			}
		}
	}

	self.skippers = newSkippers

	self.curr = result

	return
}

type interOp struct {
	skippers []skipper
	curr     *common.SetOpResult
}

func (self *interOp) skip(min []byte, inc bool) (result *common.SetOpResult, err error) {
	gt := 0
	if inc {
		gt = -1
	}

	if self.curr != nil && bytes.Compare(self.curr.Key, min) > gt {
		result = self.curr
		return
	}

	var maxKey []byte
	var results = make([]*common.SetOpResult, len(self.skippers))

	for result == nil {
		maxKey = nil
		for index, thisSkipper := range self.skippers {
			if results[index], err = thisSkipper.skip(min, inc); results[index] == nil || err != nil {
				result = nil
				self.curr = nil
				return
			}
			if maxKey == nil {
				maxKey = results[index].Key
				result = results[index].ShallowCopy()
			} else {
				result.Values = append(result.Values, results[index].Values...)
				if bytes.Compare(results[index].Key, maxKey) > 0 {
					result = nil
					maxKey = results[index].Key
				}
			}
		}

		if result != nil {
			for index, _ := range self.skippers {
				if bytes.Compare(maxKey, results[index].Key) != 0 {
					result = nil
				}
			}
		}

		min = maxKey
		inc = true
	}

	self.curr = result

	return
}

type diffOp struct {
	skippers []skipper
	curr     *common.SetOpResult
}

func (self *diffOp) skip(min []byte, inc bool) (result *common.SetOpResult, err error) {
	gt := 0
	if inc {
		gt = -1
	}

	if self.curr != nil && bytes.Compare(self.curr.Key, min) > gt {
		result = self.curr
		return
	}

	var newSkippers = make([]skipper, 0, len(self.skippers))
	var res *common.SetOpResult

	for result == nil {
		for index, thisSkipper := range self.skippers {
			if res, err = thisSkipper.skip(min, inc); err != nil {
				result = nil
				self.curr = nil
				return
			}
			if index == 0 {
				if res == nil {
					result = nil
					self.curr = nil
					return
				}
				result = res
				newSkippers = append(newSkippers, thisSkipper)
				min = res.Key
				inc = true
			} else {
				if res != nil {
					newSkippers = append(newSkippers, thisSkipper)
					if bytes.Compare(min, res.Key) == 0 {
						result = nil
						break
					}
				}
			}
		}
		self.skippers = newSkippers
		newSkippers = newSkippers[:0]
		inc = false
	}

	self.curr = result

	return
}

type treeSkipper struct {
	key          []byte
	tree         *radix.Tree
	remote       common.Remote
	buffer       []common.SetOpResult
	currentIndex int
}

func (self *treeSkipper) skip(min []byte, inc bool) (result *common.SetOpResult, err error) {
	lt := 1
	if inc {
		lt = 0
	}
	if len(self.buffer) == 0 {
		if err = self.refill(min, inc); err != nil {
			return
		}
		if len(self.buffer) == 0 {
			return
		}
	}
	result = &self.buffer[self.currentIndex]
	for min != nil && bytes.Compare(result.Key, min) < lt {
		if result, err = self.nextWithRefill(min, inc); result == nil || err != nil {
			return
		}
	}
	return
}

func (self *treeSkipper) nextWithRefill(refillMin []byte, inc bool) (result *common.SetOpResult, err error) {
	result = self.nextFromBuf()
	if result == nil {
		if err = self.refill(refillMin, inc); err != nil {
			return
		}
		if len(self.buffer) == 0 {
			return
		}
		result = &self.buffer[self.currentIndex]
	}
	return
}

func (self *treeSkipper) nextFromBuf() (result *common.SetOpResult) {
	self.currentIndex++
	if self.currentIndex < len(self.buffer) {
		result = &self.buffer[self.currentIndex]
		return
	}
	return
}

func (self *treeSkipper) refill(min []byte, inc bool) (err error) {
	self.buffer = make([]common.SetOpResult, 0, bufferSize)
	self.currentIndex = 0
	if self.tree == nil {
		if err = self.remoteRefill(min, inc); err != nil {
			return
		}
	} else {
		if err = self.treeRefill(min, inc); err != nil {
			return
		}
	}
	return
}

func (self *treeSkipper) remoteRefill(min []byte, inc bool) (err error) {
	r := common.Range{
		Key:    self.key,
		Min:    min,
		MinInc: inc,
		Len:    bufferSize,
	}
	var items []common.Item
	if err = self.remote.Call("DHash.SliceLen", r, &items); err != nil {
		return
	}
	for _, item := range items {
		self.buffer = append(self.buffer, common.SetOpResult{item.Key, [][]byte{item.Value}})
	}
	return
}

func (self *treeSkipper) treeRefill(min []byte, inc bool) error {
	filler := func(key, value []byte, timestamp int64) bool {
		self.buffer = append(self.buffer, common.SetOpResult{key, [][]byte{value}})
		return len(self.buffer) < bufferSize
	}
	self.tree.SubEachBetween(self.key, min, nil, inc, false, filler)
	return nil
}

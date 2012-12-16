package setop

import (
	"bytes"
	"fmt"
)

type Skipper interface {
	// skip returns a value matching the min and inclusive criteria.
	// If the last yielded value matches the criteria the same value will be returned again.
	Skip(min []byte, inc bool) (result *SetOpResult, err error)
}

func createSkippers(r RawSourceCreator, sources []SetOpSource) (result []Skipper) {
	result = make([]Skipper, len(sources))
	for index, source := range sources {
		if source.Key != nil {
			result[index] = r(source.Key)
		} else {
			result[index] = createSkipper(r, source.SetOp)
		}
	}
	return
}

func createSkipper(r RawSourceCreator, op *SetOp) (result Skipper) {
	switch op.Type {
	case Union:
		result = &unionOp{
			skippers: createSkippers(r, op.Sources),
			merger:   getMerger(op.Merge),
		}
	case Intersection:
		result = &interOp{
			skippers: createSkippers(r, op.Sources),
			merger:   getMerger(op.Merge),
		}
	case Difference:
		result = &diffOp{
			skippers: createSkippers(r, op.Sources),
			merger:   getMerger(op.Merge),
		}
	case Xor:
		result = &xorOp{
			skippers: createSkippers(r, op.Sources),
			merger:   getMerger(op.Merge),
		}
	default:
		panic(fmt.Errorf("Unknown SetOp Type %v", op.Type))
	}
	return
}

type xorOp struct {
	skippers []Skipper
	curr     *SetOpResult
	merger   mergeFunc
}

func (self *xorOp) Skip(min []byte, inc bool) (result *SetOpResult, err error) {
	gt := 0
	if inc {
		gt = -1
	}

	if self.curr != nil && bytes.Compare(self.curr.Key, min) > gt {
		result = self.curr
		return
	}

	newSkippers := make([]Skipper, 0, len(self.skippers))

	var res *SetOpResult
	var cmp int
	for result == nil {
		for _, thisSkipper := range self.skippers {
			if res, err = thisSkipper.Skip(min, inc); err != nil {
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
						result.Values = self.merger(result.Values, res.Values)
					}
				}
			}
		}

		if len(newSkippers) == 0 {
			result = nil
			self.curr = nil
			return
		}

		if result != nil && len(result.Values) != 1 {
			min = result.Key
			inc = false
			result = nil
		}

		self.skippers = newSkippers
		newSkippers = newSkippers[:0]

	}

	self.curr = result

	return
}

type unionOp struct {
	skippers []Skipper
	curr     *SetOpResult
	merger   mergeFunc
}

func (self *unionOp) Skip(min []byte, inc bool) (result *SetOpResult, err error) {
	gt := 0
	if inc {
		gt = -1
	}

	if self.curr != nil && bytes.Compare(self.curr.Key, min) > gt {
		result = self.curr
		return
	}

	newSkippers := make([]Skipper, 0, len(self.skippers))

	var cmp int
	var res *SetOpResult
	for _, thisSkipper := range self.skippers {
		if res, err = thisSkipper.Skip(min, inc); err != nil {
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
					result.Values = self.merger(result.Values, res.Values)
				}
			}
		}
	}

	self.skippers = newSkippers

	self.curr = result

	return
}

type interOp struct {
	skippers []Skipper
	curr     *SetOpResult
	merger   mergeFunc
}

func (self *interOp) Skip(min []byte, inc bool) (result *SetOpResult, err error) {
	gt := 0
	if inc {
		gt = -1
	}

	if self.curr != nil && bytes.Compare(self.curr.Key, min) > gt {
		result = self.curr
		return
	}

	var maxKey []byte
	var results = make([]*SetOpResult, len(self.skippers))

	for result == nil {
		maxKey = nil
		for index, thisSkipper := range self.skippers {
			if results[index], err = thisSkipper.Skip(min, inc); results[index] == nil || err != nil {
				result = nil
				self.curr = nil
				return
			}
			if maxKey == nil {
				maxKey = results[index].Key
				result = results[index].ShallowCopy()
			} else {
				result.Values = self.merger(result.Values, results[index].Values)
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
	skippers []Skipper
	curr     *SetOpResult
	merger   mergeFunc
}

func (self *diffOp) Skip(min []byte, inc bool) (result *SetOpResult, err error) {
	gt := 0
	if inc {
		gt = -1
	}

	if self.curr != nil && bytes.Compare(self.curr.Key, min) > gt {
		result = self.curr
		return
	}

	var newSkippers = make([]Skipper, 0, len(self.skippers))
	var res *SetOpResult

	for result == nil {
		for index, thisSkipper := range self.skippers {
			if res, err = thisSkipper.Skip(min, inc); err != nil {
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

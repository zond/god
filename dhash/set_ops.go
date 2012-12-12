package dhash

import (
	"../common"
	"../radix"
	"bytes"
)

const (
	bufferSize = 128
)

func skipAll(newPair *[2][]byte, newOk *bool, from []byte, inc bool, sets ...*setIter) (pairs [][2][]byte, oks []bool, err error) {
	for _, set := range sets {
		if *newPair, *newOk, err = set.skip(from, inc); err != nil {
			return
		}
		pairs = append(pairs, *newPair)
		oks = append(oks, *newOk)
	}
	return
}

func newSetOpResult(cmp *int, pairs [][2][]byte) (indices []int, result common.SetOpResult) {
	for index, pair := range pairs {
		if result.Key == nil {
			indices = []int{index}
			result.Key = pair[0]
			result.Values = [][]byte{pair[1]}
		} else {
			*cmp = bytes.Compare(pair[0], result.Key)
			if *cmp < 0 {
				indices = []int{index}
				result.Key = pair[0]
				result.Values = [][]byte{pair[1]}
			} else if *cmp == 0 {
				indices = append(indices, index)
				result.Values = append(result.Values, pair[1])
			}
		}
	}
	return
}

func skipIndices(indices []int, from []byte, sets []*setIter, pairs [][2][]byte, oks []bool) (newSets []*setIter, newPairs [][2][]byte, newOks []bool, skippedFirst bool, err error) {
	newSets = make([]*setIter, 0, len(sets))
	newPairs = make([][2][]byte, 0, len(pairs))
	newOks = make([]bool, 0, len(oks))
	for _, index := range indices {
		if pairs[index], oks[index], err = sets[index].skip(from, false); err != nil {
			return
		}
	}
	for index, ok := range oks {
		if ok {
			newSets = append(newSets, sets[index])
			newPairs = append(newPairs, pairs[index])
			newOks = append(newOks, oks[index])
		} else if index == 0 {
			skippedFirst = true
		}
	}
	return
}

type KeyValuesIterator func(res common.SetOpResult) (cont bool)

func eachUnion(f KeyValuesIterator, sets ...*setIter) (err error) {
	var preAllocPair [2][]byte
	var preAllocOk bool
	var preAllocCmp int

	var indices []int
	var setOpResult common.SetOpResult
	var pairs [][2][]byte
	var oks []bool

	if pairs, oks, err = skipAll(&preAllocPair, &preAllocOk, nil, true, sets...); err != nil {
		return
	}
	for len(oks) > 0 {
		indices, setOpResult = newSetOpResult(&preAllocCmp, pairs)
		if !f(setOpResult) {
			return
		}
		if sets, pairs, oks, _, err = skipIndices(indices, setOpResult.Key, sets, pairs, oks); err != nil {
			return
		}
	}
	return
}

func eachInter(f KeyValuesIterator, sets ...*setIter) (err error) {
	var preAllocPair [2][]byte
	var preAllocOk bool
	var preAllocCmp int

	var indices []int
	var setOpResult common.SetOpResult
	var pairs [][2][]byte
	var oks []bool

	originalSetLen := len(sets)

	if pairs, oks, err = skipAll(&preAllocPair, &preAllocOk, nil, true, sets...); err != nil {
		return
	}
	for len(oks) == originalSetLen {
		indices, setOpResult = newSetOpResult(&preAllocCmp, pairs)
		if len(indices) == len(sets) {
			if !f(setOpResult) {
				return
			}
		}
		if sets, pairs, oks, _, err = skipIndices(indices, setOpResult.Key, sets, pairs, oks); err != nil {
			return
		}
	}
	return
}

func eachDiff(f KeyValuesIterator, sets ...*setIter) (err error) {
	var preAllocPair [2][]byte
	var preAllocOk bool
	var preAllocCmp int

	var indices []int
	var setOpResult common.SetOpResult
	var pairs [][2][]byte
	var oks []bool

	var skippedFirst bool

	if pairs, oks, err = skipAll(&preAllocPair, &preAllocOk, nil, true, sets...); err != nil {
		return
	}
	for {
		indices, setOpResult = newSetOpResult(&preAllocCmp, pairs)
		if len(indices) == 1 && indices[0] == 0 {
			if !f(setOpResult) {
				return
			}
		}
		if sets, pairs, oks, skippedFirst, err = skipIndices(indices, setOpResult.Key, sets, pairs, oks); err != nil {
			return
		}
		if skippedFirst {
			return
		}
	}
	return
}

type setIter struct {
	key       []byte
	tree      *radix.Tree
	remote    common.Remote
	buffer    [][2][]byte
	nextIndex int
}

func (self *setIter) remoteRefill(from []byte, inc bool) (err error) {
	r := common.Range{
		Key:    self.key,
		Min:    from,
		MinInc: inc,
		Len:    bufferSize,
	}
	var items []common.Item
	if err = self.remote.Call("DHash.SliceLen", r, &items); err != nil {
		return
	}
	for _, item := range items {
		self.buffer = append(self.buffer, [2][]byte{item.Key, item.Value})
	}
	return
}

func (self *setIter) treeRefill(from []byte, inc bool) error {
	filler := func(key, value []byte, timestamp int64) bool {
		self.buffer = append(self.buffer, [2][]byte{key, value})
		return len(self.buffer) < bufferSize
	}
	self.tree.SubEachBetween(self.key, from, nil, inc, false, filler)
	return nil
}

func (self *setIter) refill(from []byte, inc bool) (ok bool, err error) {
	self.buffer = make([][2][]byte, 0, bufferSize)
	self.nextIndex = 0
	if self.tree == nil {
		if err = self.remoteRefill(from, inc); err != nil {
			return
		}
	} else {
		if err = self.treeRefill(from, inc); err != nil {
			return
		}
	}
	ok = len(self.buffer) > 0
	return
}

func (self *setIter) nextFromBuf() (result [2][]byte, ok bool) {
	if ok = self.nextIndex < len(self.buffer); ok {
		result = self.buffer[self.nextIndex]
		self.nextIndex++
	}
	return
}

func (self *setIter) nextWithRefill(refillFrom []byte, inc bool) (result [2][]byte, ok bool, err error) {
	result, ok = self.nextFromBuf()
	if !ok {
		if ok, err = self.refill(refillFrom, inc); !ok || err != nil {
			return
		}
		result, ok = self.nextFromBuf()
	}
	return
}

func (self *setIter) skip(from []byte, inc bool) (result [2][]byte, ok bool, err error) {
	lt := 1
	if inc {
		lt = 0
	}
	if result, ok, err = self.nextWithRefill(from, inc); !ok || err != nil {
		return
	}
	for from != nil && bytes.Compare(result[0], from) < lt {
		if result, ok, err = self.nextWithRefill(from, inc); !ok || err != nil {
			return
		}
	}
	return
}

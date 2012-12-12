package dhash

import (
	"../common"
	"../radix"
	"bytes"
)

const (
	bufferSize = 128
)

// skipAll skips all given sets to new posistions.
// It allocates as little as possible, and therefore gets pre allocated values as arguments.
func skipAll(newPair *[2][]byte, newOk *bool, from []byte, inc bool, sets []*setIter, pairs *[][2][]byte, oks *[]bool, err *error) {
	for _, set := range sets {
		if *newPair, *newOk, *err = set.skip(from, inc); *err != nil {
			return
		}
		*pairs = append(*pairs, *newPair)
		*oks = append(*oks, *newOk)
	}
	return
}

// newSetOpResult creates a new common.SetOpResult based the lowest keys in the given pairs
// It also sets whether the lowest keys include the key from the first set, 
// the indices in the set slice that had the lowest key,
// the indices in the set that didn't have the last key
// It allocates as little as possible, and therefore gets pre allocated values as arguments.
func newSetOpResult(cmp *int, pairs [][2][]byte, includesFirst *bool, last *[]byte, firstIndices, notLastIndices *[]int) (result common.SetOpResult) {
	for index, pair := range pairs {
		if *last == nil {
			*last = pair[0]
		} else if bytes.Compare(pair[0], *last) > 0 {
			*last = pair[0]
		}
		if result.Key == nil {
			*includesFirst = true
			*firstIndices = []int{index}
			result.Key = pair[0]
			result.Values = [][]byte{pair[1]}
		} else {
			*cmp = bytes.Compare(pair[0], result.Key)
			if *cmp < 0 {
				*includesFirst = false
				*firstIndices = []int{index}
				result.Key = pair[0]
				result.Values = [][]byte{pair[1]}
			} else if *cmp == 0 {
				*firstIndices = append(*firstIndices, index)
				result.Values = append(result.Values, pair[1])
			}
		}
	}
	for index, pair := range pairs {
		if bytes.Compare(pair[0], *last) != 0 {
			*notLastIndices = append(*notLastIndices, index)
		}
	}
	return
}

// skipIndices skips the given indices at the given sets to new positions.
// It also updates the sets, pairs and oks provided to only include those that returned ok.
// It allocates as little as possible, and therefore gets pre allocated values as arguments.
func skipIndices(indices []int, from []byte, inc bool, sets, newSets *[]*setIter, pairs, newPairs *[][2][]byte, oks, newOks *[]bool, err *error) {
	if indices == nil {
		for index, set := range *sets {
			if (*pairs)[index], (*oks)[index], *err = set.skip(from, inc); *err != nil {
				return
			}
		}
	} else {
		for _, index := range indices {
			if (*pairs)[index], (*oks)[index], *err = (*sets)[index].skip(from, inc); *err != nil {
				return
			}
		}
	}
	*newSets = make([]*setIter, 0, len(*sets))
	*newPairs = make([][2][]byte, 0, len(*pairs))
	*newOks = make([]bool, 0, len(*oks))
	for index, ok := range *oks {
		if ok {
			*newSets = append(*newSets, (*sets)[index])
			*newPairs = append(*newPairs, (*pairs)[index])
			*newOks = append(*newOks, (*oks)[index])
		}
	}
	*sets = *newSets
	*pairs = *newPairs
	*oks = *newOks
	return
}

type KeyValuesIterator func(res common.SetOpResult) (cont bool)

// eachUnion will call a KeyValuesIterator with all keys and values in all provided sets, once per unique key.
func eachUnion(f KeyValuesIterator, sets ...*setIter) (err error) {
	var preAllocPair [2][]byte
	var preAllocOk bool
	var preAllocCmp int
	var preAllocNewSets []*setIter
	var preAllocNewPairs [][2][]byte
	var preAllocNewOks []bool

	var x1 bool
	var x2 []byte
	var x3 []int

	var firstIndices []int
	var setOpResult common.SetOpResult
	var pairs [][2][]byte
	var oks []bool

	if skipAll(&preAllocPair, &preAllocOk, nil, true, sets, &pairs, &oks, &err); err != nil {
		return
	}
	for len(oks) > 0 {
		setOpResult = newSetOpResult(&preAllocCmp, pairs, &x1, &x2, &firstIndices, &x3)
		if !f(setOpResult) {
			return
		}
		if skipIndices(firstIndices, setOpResult.Key, false, &sets, &preAllocNewSets, &pairs, &preAllocNewPairs, &oks, &preAllocNewOks, &err); err != nil {
			return
		}
	}
	return
}

// eachInter will call a KeyValuesIterator once per key that is present in all provided sets.
func eachInter(f KeyValuesIterator, sets ...*setIter) (err error) {
	var preAllocPair [2][]byte
	var preAllocOk bool
	var preAllocCmp int
	var preAllocNewSets []*setIter
	var preAllocNewPairs [][2][]byte
	var preAllocNewOks []bool

	var x1 bool
	var x2 []int

	var notLastIndices []int
	var setOpResult common.SetOpResult
	var pairs [][2][]byte
	var oks []bool
	var last []byte

	originalSetLen := len(sets)

	if skipAll(&preAllocPair, &preAllocOk, nil, true, sets, &pairs, &oks, &err); err != nil {
		return
	}
	for len(oks) == originalSetLen {
		setOpResult = newSetOpResult(&preAllocCmp, pairs, &x1, &last, &x2, &notLastIndices)
		if len(setOpResult.Values) == originalSetLen {
			if !f(setOpResult) {
				return
			}
			if skipIndices(nil, last, true, &sets, &preAllocNewSets, &pairs, &preAllocNewPairs, &oks, &preAllocNewOks, &err); err != nil {
				return
			}
		} else {
			if skipIndices(notLastIndices, last, true, &sets, &preAllocNewSets, &pairs, &preAllocNewPairs, &oks, &preAllocNewOks, &err); err != nil {
				return
			}
		}
	}
	return
}

// eachDiff will call a KeyValuesIterator once per key present in the first provided set that is not present in the second and up.
func eachDiff(f KeyValuesIterator, sets ...*setIter) (err error) {
	var preAllocPair [2][]byte
	var preAllocOk bool
	var preAllocCmp int
	var preAllocNewSets []*setIter
	var preAllocNewPairs [][2][]byte
	var preAllocNewOks []bool

	var x1 []byte
	var x2 []int

	var firstIndices []int
	var setOpResult common.SetOpResult
	var pairs [][2][]byte
	var oks []bool

	var includesFirst bool

	if skipAll(&preAllocPair, &preAllocOk, nil, true, sets, &pairs, &oks, &err); err != nil {
		return
	}
	for {
		setOpResult = newSetOpResult(&preAllocCmp, pairs, &includesFirst, &x1, &firstIndices, &x2)
		if includesFirst {
			if len(firstIndices) == 1 {
				if !f(setOpResult) {
					return
				}
			}
			if pairs[0], oks[0], err = sets[0].skip(pairs[0][0], false); err != nil || !oks[0] {
				return
			}
		} else {
			if skipIndices(firstIndices, pairs[0][0], true, &sets, &preAllocNewSets, &pairs, &preAllocNewPairs, &oks, &preAllocNewOks, &err); err != nil {
				return
			}
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

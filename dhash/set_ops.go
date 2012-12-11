package dhash

import (
	"../common"
	"../radix"
	"bytes"
)

const (
	bufferSize = 128
)

type KeyValueIterator func(key, value []byte) (cont bool)

func eachUnion(s1, s2 *setIter, f KeyValueIterator) (err error) {
	var d1, d2 [2][]byte
	var ok1, ok2 bool
	if d1, ok1, err = s1.skip(nil, true); err != nil {
		return
	}
	if d2, ok2, err = s2.skip(nil, true); err != nil {
		return
	}
	cmp := 0
	for ok1 && ok2 {
		cmp = bytes.Compare(d1[0], d2[0])
		if cmp < 0 {
			if !f(d1[0], d1[1]) {
				return
			}
			if d1, ok1, err = s1.skip(d1[0], false); err != nil {
				return
			}
		} else if cmp > 0 {
			if !f(d2[0], d2[1]) {
				return
			}
			if d2, ok2, err = s2.skip(d2[0], false); err != nil {
				return
			}
		} else {
			if !f(d1[0], d1[1]) {
				return
			}
			if d1, ok1, err = s1.skip(d1[0], false); err != nil {
				return
			}
			if d2, ok2, err = s2.skip(d2[0], false); err != nil {
				return
			}
		}
	}
	for ok1 {
		if !f(d1[0], d1[1]) {
			return
		}
		if d1, ok1, err = s1.skip(d1[0], false); err != nil {
			return
		}
	}
	for ok2 {
		if !f(d2[0], d2[1]) {
			return
		}
		if d2, ok2, err = s2.skip(d2[0], false); err != nil {
			return
		}
	}
	return
}

func eachInter(s1, s2 *setIter, f KeyValueIterator) (err error) {
	var d1, d2 [2][]byte
	var ok1, ok2 bool
	if d1, ok1, err = s1.skip(nil, true); err != nil {
		return
	}
	if d2, ok2, err = s2.skip(nil, true); err != nil {
		return
	}
	cmp := 0
	for ok1 && ok2 {
		cmp = bytes.Compare(d1[0], d2[0])
		if cmp < 0 {
			if d1, ok1, err = s1.skip(d2[0], true); err != nil {
				return
			}
		} else if cmp > 0 {
			if d2, ok2, err = s2.skip(d1[0], true); err != nil {
				return
			}
		} else {
			if !f(d1[0], d1[1]) {
				return
			}
			if d1, ok1, err = s1.skip(d1[0], false); err != nil {
				return
			}
			if d2, ok2, err = s2.skip(d2[0], false); err != nil {
				return
			}
		}
	}
	return
}

func eachDiff(s1, s2 *setIter, f KeyValueIterator) (err error) {
	var d1, d2 [2][]byte
	var ok1, ok2 bool
	if d1, ok1, err = s1.skip(nil, true); err != nil {
		return
	}
	if d2, ok2, err = s2.skip(nil, true); err != nil {
		return
	}
	cmp := 0
	for ok1 && ok2 {
		cmp = bytes.Compare(d1[0], d2[0])
		if cmp > 0 {
			if d2, ok2, err = s2.skip(d2[0], false); err != nil {
				return
			}
		} else if cmp < 0 {
			if !f(d1[0], d1[1]) {
				return
			}
			if d1, ok1, err = s1.skip(d1[0], false); err != nil {
				return
			}
		} else {
			if d1, ok1, err = s1.skip(d1[0], false); err != nil {
				return
			}
			if d2, ok2, err = s2.skip(d2[0], false); err != nil {
				return
			}
		}
	}
	for ok1 {
		if !f(d1[0], d1[1]) {
			return
		}
		if d1, ok1, err = s1.skip(d1[0], false); err != nil {
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

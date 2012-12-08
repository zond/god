package radix

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sync"
)

type TreeIterator func(key []byte, value []byte, version int64) (cont bool)

type TreeIndexIterator func(key []byte, value []byte, version int64, index int) (cont bool)

func cmps(mininc, maxinc bool) (mincmp, maxcmp int) {
	if mininc {
		mincmp = -1
	}
	if maxinc {
		maxcmp = 1
	}
	return
}

func newNodeIterator(f TreeIterator) nodeIterator {
	return func(key []byte, bValue []byte, tValue *Tree, use int, version int64) (cont bool) {
		if use&byteValue != 0 {
			return f(key, bValue, version)
		}
		return true
	}
}

func newNodeIndexIterator(f TreeIndexIterator) nodeIndexIterator {
	return func(key []byte, bValue []byte, tValue *Tree, use int, version int64, index int) (cont bool) {
		if use&byteValue != 0 {
			return f(key, bValue, version, index)
		}
		return true
	}
}

// Tree defines a more specialized wrapper around the node structure.
// It contains an RWMutex to make it thread safe, and it defines a simplified and limited access API.
type Tree struct {
	lock *sync.RWMutex
	root *node
}

func NewTree() (result *Tree) {
	result = &Tree{
		lock: new(sync.RWMutex),
	}
	result.root, _, _, _, _ = result.root.insert(nil, newNode(nil, nil, nil, 0, true, 0), 0, 0)
	return
}
func newTreeWith(key []Nibble, byteValue []byte, version int64) (result *Tree) {
	result = NewTree()
	result.PutVersion(key, byteValue, 0, version)
	return
}
func (self *Tree) Each(f TreeIterator) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.each(nil, byteValue, newNodeIterator(f))
}
func (self *Tree) ReverseEach(f TreeIterator) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.reverseEach(nil, byteValue, newNodeIterator(f))
}
func (self *Tree) EachBetween(min, max []byte, mininc, maxinc bool, f TreeIterator) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	mincmp, maxcmp := cmps(mininc, maxinc)
	self.root.eachBetween(nil, Rip(min), Rip(max), mincmp, maxcmp, byteValue, newNodeIterator(f))
}
func (self *Tree) ReverseEachBetween(min, max []byte, mininc, maxinc bool, f TreeIterator) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	mincmp, maxcmp := cmps(mininc, maxinc)
	self.root.reverseEachBetween(nil, Rip(min), Rip(max), mincmp, maxcmp, byteValue, newNodeIterator(f))
}
func (self *Tree) IndexOf(key []byte) (index int, existed bool) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	index, ex := self.root.indexOf(0, Rip(key), byteValue, true)
	existed = ex&byteValue != 0
	return
}
func (self *Tree) ReverseIndexOf(key []byte) (index int, existed bool) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	index, ex := self.root.indexOf(0, Rip(key), byteValue, false)
	existed = ex&byteValue != 0
	return
}
func (self *Tree) EachBetweenIndex(min, max *int, f TreeIndexIterator) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.eachBetweenIndex(nil, 0, min, max, byteValue, newNodeIndexIterator(f))
}
func (self *Tree) ReverseEachBetweenIndex(min, max *int, f TreeIndexIterator) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.reverseEachBetweenIndex(nil, 0, min, max, byteValue, newNodeIndexIterator(f))
}
func (self *Tree) Hash() []byte {
	if self == nil {
		return nil
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.root.hash
}
func (self *Tree) ToMap() (result map[string][]byte) {
	if self == nil {
		return
	}
	result = make(map[string][]byte)
	self.Each(func(key []byte, value []byte, version int64) bool {
		result[hex.EncodeToString(key)] = value
		return true
	})
	return
}
func (self *Tree) String() string {
	if self == nil {
		return ""
	}
	return fmt.Sprint(self.ToMap())
}
func (self *Tree) SizeBetween(min, max []byte, mininc, maxinc bool) int {
	if self == nil {
		return 0
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	mincmp, maxcmp := cmps(mininc, maxinc)
	return self.root.sizeBetween(nil, Rip(min), Rip(max), mincmp, maxcmp, byteValue|treeValue)
}
func (self *Tree) Size() int {
	if self == nil {
		return 0
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.root.byteSize + self.root.treeSize
}
func (self *Tree) describeIndented(first, indent int) string {
	if self == nil {
		return ""
	}
	indentation := &bytes.Buffer{}
	for i := 0; i < first; i++ {
		fmt.Fprint(indentation, " ")
	}
	buffer := bytes.NewBufferString(fmt.Sprintf("%v<Radix size:%v hash:%v>\n", indentation, self.Size(), hex.EncodeToString(self.Hash())))
	self.root.describe(indent+2, buffer)
	return string(buffer.Bytes())
}
func (self *Tree) Describe() string {
	if self == nil {
		return ""
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.describeIndented(0, 0)
}

func (self *Tree) put(key []Nibble, byteValue []byte, treeValue *Tree, use, clear int, version int64) (oldBytes []byte, oldTree *Tree, existed int) {
	self.root, oldBytes, oldTree, _, existed = self.root.insert(nil, newNode(key, byteValue, treeValue, version, false, use), use, clear)
	return
}
func (self *Tree) Put(key []byte, bValue []byte, version int64) (oldBytes []byte, existed bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	oldBytes, _, ex := self.put(Rip(key), bValue, nil, byteValue, 0, version)
	existed = ex*byteValue != 0
	return
}
func (self *Tree) Get(key []byte) (bValue []byte, version int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	bValue, _, version, ex := self.root.get(Rip(key))
	existed = ex&byteValue != 0
	return
}
func (self *Tree) Prev(key []byte) (prevKey []byte, prevBytes []byte, prevVersion int64, existed bool) {
	self.ReverseEachBetween(nil, key, true, false, func(k []byte, b []byte, ver int64) bool {
		prevKey, prevBytes, prevVersion, existed = k, b, ver, true
		return false
	})
	return
}
func (self *Tree) Next(key []byte) (nextKey []byte, nextBytes []byte, nextVersion int64, existed bool) {
	self.EachBetween(key, nil, false, true, func(k []byte, b []byte, ver int64) bool {
		nextKey, nextBytes, nextVersion, existed = k, b, ver, true
		return false
	})
	return
}
func (self *Tree) NextIndex(index int) (key []byte, byteValue []byte, version int64, ind int, existed bool) {
	self.EachBetweenIndex(&index, nil, func(k []byte, b []byte, ver int64, i int) bool {
		key, byteValue, version, ind, existed = k, b, ver, i, true
		return false
	})
	return
}
func (self *Tree) PrevIndex(index int) (key []byte, byteValue []byte, version int64, ind int, existed bool) {
	self.ReverseEachBetweenIndex(&index, nil, func(k []byte, b []byte, ver int64, i int) bool {
		key, byteValue, version, ind, existed = k, b, ver, i, true
		return false
	})
	return
}
func (self *Tree) First() (key []byte, byteValue []byte, version int64, existed bool) {
	self.Each(func(k []byte, b []byte, ver int64) bool {
		key, byteValue, version, existed = k, b, ver, true
		return false
	})
	return
}
func (self *Tree) Last() (key []byte, byteValue []byte, version int64, existed bool) {
	self.ReverseEach(func(k []byte, b []byte, ver int64) bool {
		key, byteValue, version, existed = k, b, ver, true
		return false
	})
	return
}
func (self *Tree) Index(n int) (key []byte, byteValue []byte, version int64, existed bool) {
	self.EachBetweenIndex(&n, &n, func(k []byte, b []byte, ver int64, index int) bool {
		key, byteValue, version, existed = k, b, ver, true
		return false
	})
	return
}
func (self *Tree) ReverseIndex(n int) (key []byte, byteValue []byte, version int64, existed bool) {
	self.ReverseEachBetweenIndex(&n, &n, func(k []byte, b []byte, ver int64, index int) bool {
		key, byteValue, version, existed = k, b, ver, true
		return false
	})
	return
}
func (self *Tree) Del(key []byte, version int64) (oldBytes []byte, existed bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	oldBytes, _, ex := self.put(Rip(key), nil, nil, byteValue, byteValue, version)
	existed = ex&byteValue != 0
	return
}

func (self *Tree) SubReverseIndexOf(key, subKey []byte) (index int, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(Rip(key)); ex&treeValue != 0 && subTree != nil {
		index, existed = subTree.ReverseIndexOf(subKey)
	}
	return
}
func (self *Tree) SubIndexOf(key, subKey []byte) (index int, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(Rip(key)); ex&treeValue != 0 && subTree != nil {
		index, existed = subTree.IndexOf(subKey)
	}
	return
}
func (self *Tree) SubPrevIndex(key []byte, index int) (foundKey []byte, foundBytes []byte, foundVersion int64, foundIndex int, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(Rip(key)); ex&treeValue != 0 && subTree != nil {
		foundKey, foundBytes, foundVersion, foundIndex, existed = subTree.PrevIndex(index)
	}
	return
}
func (self *Tree) SubNextIndex(key []byte, index int) (foundKey []byte, foundBytes []byte, foundVersion int64, foundIndex int, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(Rip(key)); ex&treeValue != 0 && subTree != nil {
		foundKey, foundBytes, foundVersion, foundIndex, existed = subTree.NextIndex(index)
	}
	return
}
func (self *Tree) SubFirst(key []byte) (firstKey []byte, firstBytes []byte, firstVersion int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(Rip(key)); ex&treeValue != 0 && subTree != nil {
		firstKey, firstBytes, firstVersion, existed = subTree.First()
	}
	return
}
func (self *Tree) SubLast(key []byte) (lastKey []byte, lastBytes []byte, lastVersion int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(Rip(key)); ex&treeValue != 0 && subTree != nil {
		lastKey, lastBytes, lastVersion, existed = subTree.Last()
	}
	return
}
func (self *Tree) SubPrev(key, subKey []byte) (prevKey []byte, prevBytes []byte, prevVersion int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(Rip(key)); ex&treeValue != 0 && subTree != nil {
		prevKey, prevBytes, prevVersion, existed = subTree.Prev(subKey)
	}
	return
}
func (self *Tree) SubNext(key, subKey []byte) (nextKey []byte, nextBytes []byte, nextVersion int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(Rip(key)); ex&treeValue != 0 && subTree != nil {
		nextKey, nextBytes, nextVersion, existed = subTree.Next(subKey)
	}
	return
}
func (self *Tree) SubSizeBetween(key, min, max []byte, mininc, maxinc bool) (result int) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(Rip(key)); ex&treeValue != 0 && subTree != nil {
		result = subTree.SizeBetween(min, max, mininc, maxinc)
	}
	return
}
func (self *Tree) SubGet(key, subKey []byte) (byteValue []byte, version int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(Rip(key)); ex&treeValue != 0 && subTree != nil {
		byteValue, version, existed = subTree.Get(subKey)
	}
	return
}
func (self *Tree) SubReverseEachBetween(key, min, max []byte, mininc, maxinc bool, f TreeIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(Rip(key)); ex&treeValue != 0 && subTree != nil {
		subTree.ReverseEachBetween(min, max, mininc, maxinc, f)
	}
}
func (self *Tree) SubEachBetween(key, min, max []byte, mininc, maxinc bool, f TreeIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(Rip(key)); ex&treeValue != 0 && subTree != nil {
		subTree.EachBetween(min, max, mininc, maxinc, f)
	}
}
func (self *Tree) SubReverseEachBetweenIndex(key []byte, min, max *int, f TreeIndexIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(Rip(key)); ex&treeValue != 0 && subTree != nil {
		subTree.ReverseEachBetweenIndex(min, max, f)
	}
}
func (self *Tree) SubEachBetweenIndex(key []byte, min, max *int, f TreeIndexIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(Rip(key)); ex&treeValue != 0 && subTree != nil {
		subTree.EachBetweenIndex(min, max, f)
	}
}
func (self *Tree) SubPut(key, subKey []byte, byteValue []byte, version int64) (oldBytes []byte, existed bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	ripped := Rip(key)
	_, subTree, _, ex := self.root.get(ripped)
	if ex&treeValue == 0 || subTree == nil {
		subTree = newTreeWith(Rip(subKey), byteValue, version)
	} else {
		oldBytes, existed = subTree.Put(subKey, byteValue, version)
	}
	self.put(ripped, nil, subTree, treeValue, 0, version)
	return
}
func (self *Tree) SubDel(key, subKey []byte, version int64) (oldBytes []byte, existed bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	ripped := Rip(key)
	_, subTree, _, ex := self.root.get(ripped)
	if ex&treeValue != 0 && subTree != nil {
		oldBytes, existed = subTree.Del(subKey, version)
		if subTree.Size() == 0 {
			self.put(ripped, nil, nil, treeValue, treeValue, version)
		} else {
			self.put(ripped, nil, subTree, treeValue, 0, version)
		}
	}
	return
}

func (self *Tree) Finger(key []Nibble) *Print {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.root.finger(&Print{}, key)
}
func (self *Tree) GetVersion(key []Nibble) (bValue []byte, version int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	bValue, _, version, ex := self.root.get(key)
	existed = ex&byteValue != 0
	return
}
func (self *Tree) putVersion(key []Nibble, bValue []byte, treeValue *Tree, use int, expected, version int64) bool {
	if _, _, current, ex := self.root.get(key); ex&byteValue != 0 || current == expected {
		self.root, _, _, _, _ = self.root.insert(nil, newNode(key, bValue, treeValue, version, false, use), use, 0)
		return true
	}
	return false
}
func (self *Tree) PutVersion(key []Nibble, bValue []byte, expected, version int64) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	return self.putVersion(key, bValue, nil, byteValue, expected, version)
}
func (self *Tree) delVersion(key []Nibble, use int, expected int64) bool {
	if _, _, current, ex := self.root.get(key); ex&use != 0 && current == expected {
		self.root, _, _, _ = self.root.del(nil, key, use)
		return true
	}
	return false
}
func (self *Tree) DelVersion(key []Nibble, expected int64) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	return self.delVersion(key, byteValue, expected)
}

func (self *Tree) SubFinger(key, subKey []Nibble, expected int64) (result *Print) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, subTreeVersion, ex := self.root.get(key); ex&treeValue != 0 && subTree != nil && subTreeVersion == expected {
		result = subTree.Finger(subKey)
	} else {
		result = &Print{}
	}
	return
}
func (self *Tree) SubGetVersion(key, subKey []Nibble, expected int64) (byteValue []byte, version int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, subTreeVersion, ex := self.root.get(key); ex&treeValue != 0 && subTree != nil && subTreeVersion == expected {
		byteValue, version, existed = subTree.GetVersion(subKey)
	}
	return
}
func (self *Tree) SubPutVersion(key, subKey []Nibble, bValue []byte, expected, subExpected, subVersion int64) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	if _, subTree, subTreeVersion, ex := self.root.get(key); ex&treeValue == 0 || (ex&treeValue != 0 && subTree != nil && subTreeVersion == expected) {
		if subTree == nil {
			subTree = newTreeWith(subKey, bValue, subVersion)
		} else {
			subTree.PutVersion(subKey, bValue, subExpected, subVersion)
		}
		self.putVersion(key, nil, subTree, treeValue, expected, expected)
		return true
	}
	return false
}
func (self *Tree) SubDelVersion(key, subKey []Nibble, expected, subExpected int64) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	if _, subTree, subTreeVersion, ex := self.root.get(key); ex&treeValue != 0 && subTree != nil && subTreeVersion == expected {
		subTree.DelVersion(subKey, subExpected)
		if subTree.Size() == 0 {
			self.delVersion(key, treeValue, expected)
		} else {
			self.putVersion(key, nil, subTree, treeValue, expected, expected)
		}
		return true
	}
	return false
}

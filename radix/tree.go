package radix

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sync"
)

func cmps(mininc, maxinc bool) (mincmp, maxcmp int) {
	if mininc {
		mincmp = -1
	}
	if maxinc {
		maxcmp = 1
	}
	return
}

type Tree struct {
	lock *sync.RWMutex
	root *node
}

func NewTree() (result *Tree) {
	result = &Tree{
		lock: new(sync.RWMutex),
		root: newNode(nil, nil, 0, false),
	}
	result.root.rehash(nil)
	return
}
func newTreeWith(key []Nibble, value Hasher, version int64) (result *Tree) {
	result = NewTree()
	result.PutVersion(key, value, 0, version)
	return
}
func (self *Tree) ReverseEach(f TreeIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.reverseEach(nil, f)
}
func (self *Tree) Each(f TreeIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.each(nil, f)
}
func (self *Tree) ReverseEachBetween(min, max []byte, mininc, maxinc bool, f TreeIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	mincmp, maxcmp := cmps(mininc, maxinc)
	self.root.reverseEachBetween(nil, rip(min), rip(max), mincmp, maxcmp, f)
}
func (self *Tree) IndexOf(key []byte) (index int, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.root.indexOf(0, rip(key))
}
func (self *Tree) EachBetween(min, max []byte, mininc, maxinc bool, f TreeIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	mincmp, maxcmp := cmps(mininc, maxinc)
	self.root.eachBetween(nil, rip(min), rip(max), mincmp, maxcmp, f)
}
func (self *Tree) ReverseEachBetweenIndex(min, max *int, f TreeIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.reverseEachBetweenIndex(nil, 0, min, max, f)
}
func (self *Tree) EachBetweenIndex(min, max *int, f TreeIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.eachBetweenIndex(nil, 0, min, max, f)
}
func (self *Tree) Hash() []byte {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.root.hash
}
func (self *Tree) ToMap() (result map[string]Hasher) {
	result = make(map[string]Hasher)
	self.Each(func(key []byte, value Hasher, version int64) bool {
		result[hex.EncodeToString(key)] = value
		return true
	})
	return
}
func (self *Tree) String() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("radix.Tree["))
	self.Each(func(key []byte, value Hasher, version int64) bool {
		fmt.Fprintf(buffer, "%v:%v, ", hex.EncodeToString(key), value)
		return true
	})
	fmt.Fprint(buffer, "]")
	return string(buffer.Bytes())
}
func (self *Tree) Size() int {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.root.size
}
func (self *Tree) describeIndented(indent int) string {
	indentation := &bytes.Buffer{}
	for i := 0; i < indent; i++ {
		fmt.Fprint(indentation, " ")
	}
	buffer := bytes.NewBufferString(fmt.Sprintf("%v<Radix size:%v hash:%v>\n", indentation, self.Size(), hex.EncodeToString(self.Hash())))
	self.root.describe(indent+2, buffer)
	return string(buffer.Bytes())
}
func (self *Tree) Describe() string {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.describeIndented(0)
}

func (self *Tree) put(key []Nibble, value Hasher, version int64) (old Hasher, existed bool) {
	self.root, old, _, existed = self.root.insert(nil, newNode(key, value, version, true))
	return
}
func (self *Tree) Put(key []byte, value Hasher, version int64) (old Hasher, existed bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	return self.put(rip(key), value, version)
}
func (self *Tree) Get(key []byte) (value Hasher, version int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	value, version, existed = self.root.get(rip(key))
	return
}
func (self *Tree) Prev(key []byte) (prevKey []byte, prevValue Hasher, prevVersion int64, existed bool) {
	self.ReverseEachBetween(nil, key, true, false, func(k []byte, v Hasher, ver int64) bool {
		prevKey, prevValue, prevVersion, existed = k, v, ver, true
		return false
	})
	return
}
func (self *Tree) Next(key []byte) (nextKey []byte, nextValue Hasher, nextVersion int64, existed bool) {
	self.EachBetween(key, nil, false, true, func(k []byte, v Hasher, ver int64) bool {
		nextKey, nextValue, nextVersion, existed = k, v, ver, true
		return false
	})
	return
}
func (self *Tree) First() (key []byte, value Hasher, version int64, existed bool) {
	self.Each(func(k []byte, v Hasher, ver int64) bool {
		key, value, version, existed = k, v, ver, true
		return false
	})
	return
}
func (self *Tree) Last() (key []byte, value Hasher, version int64, existed bool) {
	self.ReverseEach(func(k []byte, v Hasher, ver int64) bool {
		key, value, version, existed = k, v, ver, true
		return false
	})
	return
}
func (self *Tree) Index(n int) (key []byte, value Hasher, version int64, existed bool) {
	self.EachBetweenIndex(&n, &n, func(k []byte, v Hasher, ver int64) bool {
		key, value, version, existed = k, v, ver, true
		return false
	})
	return
}
func (self *Tree) Del(key []byte) (old Hasher, existed bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	return self.del(rip(key))
}
func (self *Tree) del(key []Nibble) (old Hasher, existed bool) {
	self.root, old, existed = self.root.del(nil, key)
	return
}

func (self *Tree) getSubTree(key []Nibble) (subTree *Tree, version int64) {
	var value Hasher
	var existed bool
	if value, version, existed = self.root.get(key); existed {
		subTree, _ = value.(*Tree)
	}
	return
}

func (self *Tree) SubPut(key, subKey []byte, value Hasher, version int64) (old Hasher, existed bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	ripped := rip(key)
	subTree, subTreeVersion := self.getSubTree(ripped)
	if subTree == nil {
		subTree = newTreeWith(rip(subKey), value, version)
	} else {
		old, existed = subTree.Put(subKey, value, version)
	}
	self.putVersion(ripped, subTree, subTreeVersion, subTreeVersion)
	return
}
func (self *Tree) SubGet(key, subKey []byte) (value Hasher, version int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if subTree, _ := self.getSubTree(rip(key)); subTree != nil {
		value, version, existed = subTree.Get(subKey)
	}
	return
}
func (self *Tree) SubDel(key, subKey []byte) (old Hasher, existed bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	ripped := rip(key)
	subTree, subTreeVersion := self.getSubTree(ripped)
	if subTree != nil {
		old, existed = subTree.Del(key)
		if subTree.Size() == 0 {
			self.del(ripped)
		} else {
			self.put(ripped, subTree, subTreeVersion)
		}
	}
	return
}

func (self *Tree) Finger(key []Nibble) *Print {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.root.finger(&Print{}, key)
}
func (self *Tree) GetVersion(key []Nibble) (value Hasher, version int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	value, version, existed = self.root.get(key)
	return
}
func (self *Tree) putVersion(key []Nibble, value Hasher, expected, version int64) bool {
	if _, current, existed := self.root.get(key); !existed || current == expected {
		self.root, _, _, existed = self.root.insert(nil, newNode(key, value, version, true))
		return true
	}
	return false
}
func (self *Tree) PutVersion(key []Nibble, value Hasher, expected, version int64) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	return self.putVersion(key, value, expected, version)
}
func (self *Tree) DelVersion(key []Nibble, expected int64) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	if _, current, existed := self.root.get(key); existed && current == expected {
		self.root, _, _ = self.root.del(nil, key)
		return true
	}
	return false
}

func (self *Tree) SubFinger(key, subKey []Nibble, expected int64) (result *Print) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if subTree, subTreeVersion := self.getSubTree(key); subTree != nil && subTreeVersion == expected {
		result = subTree.Finger(subKey)
	} else {
		result = &Print{}
	}
	return
}
func (self *Tree) SubGetVersion(key, subKey []Nibble, expected int64) (value Hasher, version int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if subTree, subTreeVersion := self.getSubTree(key); subTree != nil && subTreeVersion == expected {
		value, version, existed = subTree.GetVersion(subKey)
	}
	return
}
func (self *Tree) SubPutVersion(key, subKey []Nibble, value Hasher, expected, subExpected, subVersion int64) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	if subTree, subTreeVersion := self.getSubTree(key); subTree == nil || subTreeVersion == expected {
		if subTree == nil {
			subTree = newTreeWith(subKey, value, subVersion)
		} else {
			subTree.PutVersion(subKey, value, subExpected, subVersion)
		}
		self.putVersion(key, subTree, expected, expected)
		return true
	}
	return false
}
func (self *Tree) SubDelVersion(key, subKey []Nibble, expected, subExpected int64) bool {
	self.lock.Lock()
	defer self.lock.Unlock()
	if subTree, subTreeVersion := self.getSubTree(key); subTree != nil && subTreeVersion == expected {
		subTree.DelVersion(subKey, subExpected)
		if subTree.Size() == 0 {
			self.DelVersion(key, expected)
		} else {
			self.putVersion(key, subTree, expected, expected)
		}
		return true
	}
	return false
}

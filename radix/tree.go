package radix

import (
	"bytes"
	"encoding/hex"
	"fmt"
)

type Tree struct {
	size int
	root *node
}

func NewTree() (result *Tree) {
	result = &Tree{0, newNode(nil, nil, 0, false)}
	result.root.rehash(nil)
	return
}
func newTreeWith(key []Nibble, value Hasher, version int64) (result *Tree) {
	result = NewTree()
	result.PutVersion(key, value, 0, version)
	return
}

func (self *Tree) Each(f TreeIterator) {
	self.root.each(nil, f)
}

func (self *Tree) Hash() []byte {
	return self.root.hash
}
func (self *Tree) ToMap() (result map[string]Hasher) {
	result = make(map[string]Hasher)
	self.Each(func(key []byte, value Hasher) {
		result[hex.EncodeToString(key)] = value
	})
	return
}
func (self *Tree) String() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("radix.Tree["))
	self.Each(func(key []byte, value Hasher) {
		fmt.Fprintf(buffer, "%v:%v, ", hex.EncodeToString(key), value)
	})
	fmt.Fprint(buffer, "]")
	return string(buffer.Bytes())
}
func (self *Tree) Size() int {
	return self.size
}
func (self *Tree) describeIndented(indent int) string {
	buffer := bytes.NewBufferString(fmt.Sprintf("<Radix size:%v hash:%v>\n", self.Size(), hex.EncodeToString(self.Hash())))
	self.root.eachChild(func(node *node) {
		node.describe(indent, buffer)
	})
	return string(buffer.Bytes())
}
func (self *Tree) Describe() string {
	return self.describeIndented(2)
}

func (self *Tree) put(key []Nibble, value Hasher, version int64) (old Hasher, existed bool) {
	self.root, old, _, existed = self.root.insert(nil, newNode(key, value, version, true))
	if !existed {
		self.size++
	}
	return
}
func (self *Tree) Put(key []byte, value Hasher, version int64) (old Hasher, existed bool) {
	return self.put(rip(key), value, version)
}
func (self *Tree) Get(key []byte) (value Hasher, version int64, existed bool) {
	value, version, existed = self.root.get(rip(key))
	return
}
func (self *Tree) Del(key []byte) (old Hasher, existed bool) {
	return self.del(rip(key))
}
func (self *Tree) del(key []Nibble) (old Hasher, existed bool) {
	self.root, old, existed = self.root.del(nil, key)
	if existed {
		self.size--
	}
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
	ripped := rip(key)
	subTree, subTreeVersion := self.getSubTree(ripped)
	if subTree == nil {
		subTree = newTreeWith(rip(subKey), value, version)
	} else {
		old, existed = subTree.Put(subKey, value, version)
	}
	self.PutVersion(ripped, subTree, subTreeVersion, subTreeVersion)
	return
}
func (self *Tree) SubGet(key, subKey []byte) (value Hasher, version int64, existed bool) {
	if subTree, _ := self.getSubTree(rip(key)); subTree != nil {
		value, version, existed = subTree.Get(subKey)
	}
	return
}
func (self *Tree) SubDel(key, subKey []byte) (old Hasher, existed bool) {
	ripped := rip(key)
	if subTree, subTreeVersion := self.getSubTree(ripped); subTree != nil {
		old, existed = subTree.Del(key)
		if subTree.Size() == 0 {
			self.del(ripped)
		} else {
			self.put(ripped, subTree, subTreeVersion)
		}
	}
	return
}

func (self *Tree) Finger(key []Nibble) (result *Print) {
	return self.root.finger(&Print{nil, nil, 0, false, nil}, key)
}
func (self *Tree) GetVersion(key []Nibble) (value Hasher, version int64, existed bool) {
	value, version, existed = self.root.get(key)
	return
}
func (self *Tree) PutVersion(key []Nibble, value Hasher, expected, version int64) {
	if _, current, existed := self.root.get(key); !existed || current == expected {
		self.root, _, _, existed = self.root.insert(nil, newNode(key, value, version, true))
		if !existed {
			self.size++
		}
	}
}
func (self *Tree) DelVersion(key []Nibble, expected int64) {
	if _, current, existed := self.root.get(key); existed && current == expected {
		var existed bool
		self.root, _, existed = self.root.del(nil, key)
		if existed {
			self.size--
		}
	}
}

func (self *Tree) SubFinger(key, subKey []Nibble, expected int64) (result *Print) {
	if subTree, subTreeVersion := self.getSubTree(key); subTree != nil && subTreeVersion == expected {
		result = subTree.Finger(subKey)
	}
	return
}
func (self *Tree) SubGetVersion(key, subKey []Nibble, expected int64) (value Hasher, version int64, existed bool) {
	if subTree, subTreeVersion := self.getSubTree(key); subTree != nil && subTreeVersion == expected {
		value, version, existed = subTree.GetVersion(subKey)
	}
	return
}
func (self *Tree) SubPutVersion(key, subKey []Nibble, value Hasher, expected, subExpected, subVersion int64) {
	if subTree, subTreeVersion := self.getSubTree(key); subTree == nil || subTreeVersion == expected {
		if subTree == nil {
			subTree = newTreeWith(subKey, value, subVersion)
		} else {
			subTree.PutVersion(subKey, value, subExpected, subVersion)
		}
		self.PutVersion(key, subTree, expected, expected)
	}
	return
}
func (self *Tree) SubDelVersion(key, subKey []Nibble, expected, subExpected int64) {
	if subTree, subTreeVersion := self.getSubTree(key); subTree != nil && subTreeVersion == expected {
		subTree.DelVersion(subKey, subExpected)
		if subTree.Size() == 0 {
			self.DelVersion(key, expected)
		} else {
			self.PutVersion(key, subTree, expected, expected)
		}
	}
}

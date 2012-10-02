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

func NewTree() *Tree {
	return &Tree{0, newNode(nil, nil, 0, false)}
}
func newTreeWith(key []byte, value Hasher, version uint32) (result *Tree) {
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

func (self *Tree) Put(key []byte, value Hasher) (old Hasher, existed bool) {
	self.root, old, _, existed = self.root.insert(nil, true, newNode(rip(key), value, 0, true))
	if !existed {
		self.size++
	}
	return
}
func (self *Tree) Get(key []byte) (value Hasher, existed bool) {
	value, _, existed = self.root.get(rip(key))
	return
}
func (self *Tree) Del(key []byte) (old Hasher, existed bool) {
	self.root, old, existed = self.root.del(nil, rip(key))
	if existed {
		self.size--
	}
	return
}

func (self *Tree) getSubTree(key []byte) (subTree *Tree, version uint32) {
	var value Hasher
	var existed bool
	if value, version, existed = self.root.get(rip(key)); existed {
		subTree, _ = value.(*Tree)
	}
	return
}

func (self *Tree) SubPut(key, subKey []byte, value Hasher) (old Hasher, existed bool) {
	subTree, subTreeVersion := self.getSubTree(key)
	if subTree == nil {
		subTree = newTreeWith(subKey, value, 0)
	} else {
		old, existed = subTree.Put(subKey, value)
	}
	self.PutVersion(key, subTree, subTreeVersion, subTreeVersion)
	return
}
func (self *Tree) SubGet(key, subKey []byte) (value Hasher, existed bool) {
	if subTree, _ := self.getSubTree(key); subTree != nil {
		value, existed = subTree.Get(subKey)
	}
	return
}
func (self *Tree) SubDel(key, subKey []byte) (old Hasher, existed bool) {
	if subTree, _ := self.getSubTree(key); subTree != nil {
		old, existed = subTree.Del(key)
		if subTree.Size() == 0 {
			self.Del(key)
		} else {
			self.Put(key, subTree)
		}
	}
	return
}

func (self *Tree) Finger(key []byte) (result *Print) {
	return self.root.finger(&Print{nil, nil, 0, false, nil}, key)
}
func (self *Tree) GetVersion(key []byte) (value Hasher, version uint32, existed bool) {
	value, version, existed = self.root.get(rip(key))
	return
}
func (self *Tree) PutVersion(key []byte, value Hasher, expected, version uint32) {
	ripped := rip(key)
	if _, current, existed := self.root.get(ripped); !existed || current == expected {
		self.root, _, _, existed = self.root.insert(nil, false, newNode(ripped, value, version, true))
		if !existed {
			self.size++
		}
	}
}
func (self *Tree) DelVersion(key []byte, expected uint32) {
	ripped := rip(key)
	if _, current, existed := self.root.get(ripped); existed && current == expected {
		self.root, _, _ = self.root.del(nil, ripped)
	}
}

func (self *Tree) SubFinger(key, subKey []byte, expected uint32) (result *Print) {
	if subTree, subTreeVersion := self.getSubTree(key); subTree != nil && subTreeVersion == expected {
		result = subTree.Finger(subKey)
	}
	return
}
func (self *Tree) SubGetVersion(key, subKey []byte, expected uint32) (value Hasher, version uint32, existed bool) {
	if subTree, subTreeVersion := self.getSubTree(key); subTree != nil && subTreeVersion == expected {
		value, version, existed = subTree.GetVersion(subKey)
	}
	return
}
func (self *Tree) SubPutVersion(key, subKey []byte, value Hasher, expected, subExpected, subVersion uint32) {
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
func (self *Tree) SubDelVersion(key, subKey []byte, expected, subExpected uint32) {
	if subTree, subTreeVersion := self.getSubTree(key); subTree != nil && subTreeVersion == expected {
		subTree.DelVersion(subKey, subExpected)
		if subTree.Size() == 0 {
			self.DelVersion(key, expected)
		} else {
			self.PutVersion(key, subTree, expected, expected)
		}
	}
}

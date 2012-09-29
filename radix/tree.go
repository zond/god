package radix

import (
	"bytes"
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
	result.PutVersion(key, value, version)
	return
}

func (self *Tree) Finger(key []byte) (result *Print) {
	return self.root.finger(&Print{nil, nil, 0, nil}, key)
}
func (self *Tree) getSubTree(key []byte) (subTree *Tree, err error) {
	if value, existed := self.Get(key); existed {
		var ok bool
		if subTree, ok = value.(*Tree); !ok {
			err = fmt.Errorf("%v contains %v, not a sub tree", key, value)
		}
	}
	return
}
func (self *Tree) Put(key []byte, value Hasher) (old Hasher, existed bool) {
	self.root, old, _, existed = self.root.insert(nil, true, newNode(rip(key), value, 0, true))
	if !existed {
		self.size++
	}
	return
}
func (self *Tree) SubPut(key, subKey []byte, value Hasher) (old Hasher, existed bool, err error) {
	if subTree, err := self.getSubTree(key); err == nil {
		if subTree != nil {
			old, existed = subTree.Put(subKey, value)
		} else {
			self.Put(key, newTreeWith(subKey, value, 0))
		}
	}
	return
}
func (self *Tree) PutVersion(key []byte, value Hasher, version uint32) (old Hasher, oldVersion uint32, existed bool) {
	self.root, old, oldVersion, existed = self.root.insert(nil, false, newNode(rip(key), value, version, true))
	if !existed {
		self.size++
	}
	return
}
func (self *Tree) SubPutVersion(key, subKey []byte, value Hasher, version uint32) (old Hasher, oldVersion uint32, existed bool) {
	if subTree, err := self.getSubTree(key); err == nil {
		if subTree != nil {
			old, oldVersion, existed = subTree.PutVersion(subKey, value, version)
		} else {
			self.Put(key, newTreeWith(subKey, value, version))
		}
	}
	return
}
func (self *Tree) Hash() []byte {
	return self.root.hash
}
func (self *Tree) Get(key []byte) (value Hasher, existed bool) {
	value, _, existed = self.GetVersion(key)
	return
}
func (self *Tree) SubGet(key, subKey []byte) (value Hasher, existed bool, err error) {
	if subTree, err := self.getSubTree(key); err == nil && subTree != nil {
		value, existed = subTree.Get(subKey)
	}
	return
}
func (self *Tree) GetVersion(key []byte) (value Hasher, version uint32, existed bool) {
	return self.root.get(rip(key))
}
func (self *Tree) SubGetVersion(key, subKey []byte) (value Hasher, version uint32, existed bool, err error) {
	if subTree, err := self.getSubTree(key); err == nil && subTree != nil {
		value, version, existed = subTree.GetVersion(subKey)
	}
	return
}
func (self *Tree) Del(key []byte) (old Hasher, existed bool) {
	self.root, old, existed = self.root.del(nil, rip(key))
	if existed {
		self.size--
	}
	return
}
func (self *Tree) SubDel(key, subKey []byte) (old Hasher, existed bool, err error) {
	if subTree, err := self.getSubTree(key); err == nil && subTree != nil {
		old, existed = subTree.Del(key)
	}
	return
}
func (self *Tree) ToMap() (result map[string]Hasher) {
	result = make(map[string]Hasher)
	self.Each(func(key []byte, value Hasher) {
		result[string(key)] = value
	})
	return
}
func (self *Tree) Each(f TreeIterator) {
	self.root.each(nil, f)
}
func (self *Tree) Size() int {
	return self.size
}
func (self *Tree) Describe() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("<Radix size:%v hash:%v>\n", self.Size(), self.Hash()))
	self.root.eachChild(func(node *node) {
		node.describe(2, buffer)
	})
	return string(buffer.Bytes())
}

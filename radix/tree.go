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
	result.PutVersion(key, value, version)
	return
}

func (self *Tree) getSubTree(key []byte) (subTree *Tree, version uint32) {
	if value, version, existed := self.GetVersion(key); existed {
		subTree, _ = value.(*Tree)
	}
	return
}
func (self *Tree) Finger(key []byte) (result *Print) {
	return self.root.finger(&Print{nil, nil, 0, false, nil}, key)
}
func (self *Tree) SubFinger(key, subKey []byte, version uint32) (result *Print) {
	if subTree, subTreeVersion := self.getSubTree(key); subTree != nil && subTreeVersion == version {
		result = subTree.Finger(subKey)
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
func (self *Tree) SubPut(key, subKey []byte, value Hasher) (old Hasher, existed bool) {
	self.root, old, _, existed = self.root.insert(nil, true, newNode(rip(key), newTreeWith(subKey, value, 0), 0, true))
	if !existed {
		self.size++
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
func (self *Tree) SubPutVersion(key, subKey []byte, value Hasher, version, subVersion uint32) {
	if subTree, subTreeVersion := self.getSubTree(key); subTree != nil && subTreeVersion == version {
		self.root, _, _, existed := self.root.insert(nil, false, newNode(rip(key), newTreeWith(subKey, value, subVersion), version, true))
		if !existed {
			self.size++
		}
	}
	return
}
func (self *Tree) Hash() []byte {
	return self.root.hash
}
func (self *Tree) SubHash(key []byte) (hash []byte) {
	if subTree := self.getSubTree(key); subTree != nil {
		hash = subTree.Hash()
	}
	return
}
func (self *Tree) Get(key []byte) (value Hasher, existed bool) {
	value, _, existed = self.GetVersion(key)
	return
}
func (self *Tree) SubGet(key, subKey []byte) (value Hasher, existed bool) {
	if subTree := self.getSubTree(key); subTree != nil {
		value, existed = subTree.Get(subKey)
	}
	return
}
func (self *Tree) GetVersion(key []byte) (value Hasher, version uint32, existed bool) {
	return self.root.get(rip(key))
}
func (self *Tree) SubGetVersion(key, subKey []byte) (value Hasher, version uint32, existed bool) {
	if subTree := self.getSubTree(key); subTree != nil {
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
func (self *Tree) SubDel(key, subKey []byte) (old Hasher, existed bool) {
	if subTree := self.getSubTree(key); subTree != nil {
		old, existed = subTree.Del(key)
		if subTree.Size() == 0 {
			self.Del(key)
		}
	}
	return
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
func (self *Tree) Each(f TreeIterator) {
	self.root.each(nil, f)
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

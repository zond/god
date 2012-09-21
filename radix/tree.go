
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
	return &Tree{0, newNode(nil, nil, false)}
}

func (self *Tree) Finger(key []byte) (result *Print) {
	return self.root.finger(&Print{nil, nil, nil}, key)
}
func (self *Tree) Put(key []byte, value Hasher) (old Hasher, existed bool) {
	self.root, old, existed = self.root.insert(nil, newNode(rip(key), value, true))
	if !existed {
		self.size++
	}
	return
}
func (self *Tree) Hash() []byte {
	return self.root.hash
}
func (self *Tree) Get(key []byte) (value Hasher, existed bool) {
	return self.root.get(rip(key))
}
func (self *Tree) Del(key []byte) (old Hasher, existed bool) {
	self.root, old, existed = self.root.del(nil, rip(key))
	if existed {
		self.size--
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



package radix

import (
	"bytes"
	"fmt"
	"../murmur"
)

type Hasher interface {
	Hash() []byte
}

type StringHasher string
func (self StringHasher) Hash() []byte {
	return murmur.HashString(string(self))
}

type Tree struct {
	size int
	root *node
}
func NewTree() *Tree {
	return &Tree{0, newNode(nil, nil)}
}
func (self *Tree) Put(key []byte, value Hasher) (old Hasher, existed bool) {
	node := newNode(key, value)
	self.size++
	return self.root.insert(node)
}
func (self *Tree) Get(key []byte) (value Hasher, existed bool) {
	return self.root.get(key)
}
func (self *Tree) Size() int {
	return self.size
}
func (self *Tree) Describe() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("<Radix size:%v>\n", self.Size()))
	self.root.describe(2, buffer)
	return string(buffer.Bytes())
}

type node struct {
	key []byte
	value Hasher
	children [][][][]*node
}
func newNode(key []byte, value Hasher) *node {
	return &node{
		key: key,
		value: value,
		children: make([][][][]*node, 4),
	}
}
func (self *node) indices(i byte) (p1, p2, p3, p4 byte) {
	p1 = (i & (3 << 6)) >> 6
	p2 = (i & (3 << 4)) >> 4
	p3 = (i & (3 << 2)) >> 2
	p4 = (i & 3)
	return
}
func (self *node) childSlice(i byte) (slice []*node, subi byte) {
	p1, p2, p3, p4 := self.indices(i)
	if self.children[p1] == nil {
		self.children[p1] = make([][][]*node, 4)
	}
	if self.children[p1][p2] == nil {
		self.children[p1][p2] = make([][]*node, 4)
	}
	if self.children[p1][p2][p3] == nil {
		self.children[p1][p2][p3] = make([]*node, 4)
	}
	return self.children[p1][p2][p3], p4
}
func (self *node) getChild(i byte) *node {
	slice, subi := self.childSlice(i)
	return slice[subi]
}
func (self *node) setChild(i byte, child *node) {
	slice, subi := self.childSlice(i)
	slice[subi] = child
}
func (self *node) describe(indent int, buffer *bytes.Buffer) {
	indentation := &bytes.Buffer{}
	for i := 0; i < indent; i++ {
		fmt.Fprint(indentation, " ")
	}
	fmt.Fprintf(buffer, "%v%v", string(indentation.Bytes()), string(self.key))
	if self.value != nil {
		fmt.Fprintf(buffer, " => %v", self.value)
	}
	fmt.Fprintf(buffer, "\n")
	for _, s1 := range self.children {
		for _, s2 := range s1 {
			for _, s3 := range s2 {
				for _, node := range s3 {
					node.describe(indent + len(self.key), buffer)
				}
			}
		}
	}
}
func (self *node) trimKey(from, to int) {
	new_key := make([]byte, to - from)
	copy(new_key, self.key[from:to])
	self.key = new_key
}
func (self *node) get(key []byte) (value Hasher, existed bool) {
	if current := self.getChild(key[0]); current != nil {
		for i := 0;; i ++ {
			if i >= len(key) && i >= len(current.key) {
				value, existed = current.value, true
				return
			} else if i >= len(key) {
				return
			} else if i >= len(current.key) {
				value, existed = current.get(key[i:])
				return
			} else if key[i] != current.key[i] {
				return
			}
		}
	} 
	return
}
func (self *node) insert(node *node) (old Hasher, existed bool) {
	if current := self.getChild(node.key[0]); current == nil {
		self.setChild(node.key[0], node)
		return
	} else {
		for i := 0;; i ++ {
			if i >= len(node.key) {
				self.setChild(node.key[0], node)
				current.trimKey(i, len(current.key))
				node.setChild(current.key[0], current)
				return
			} else if i >= len(current.key) {
				node.trimKey(i, len(node.key))
				old, existed = current.insert(node)
				return
			} else if node.key[i] != current.key[i] {
				extra_node := newNode(make([]byte, i), nil)
				copy(extra_node.key, node.key[:i])
				self.setChild(extra_node.key[0], extra_node)
				node.trimKey(i, len(node.key))
				extra_node.setChild(node.key[0], node)
				current.trimKey(i, len(current.key))
				extra_node.setChild(current.key[0], current)
				return
			}
		}
	}
	panic("shouldn't happen")
}

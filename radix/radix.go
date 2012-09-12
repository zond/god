
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

type edge struct {
	key byte
	node *node
}
func (self *edge) String() string {
	return fmt.Sprintf("{%v:%v}", self.key, *self.node)
}

type edges []*edge
func newEdges() *edges {
	var rval edges
	return &rval
}
func (self *edges) get(k byte) *node {
	for _, e := range *self {
		if e.key == k {
			return e.node
		}
	}
	return nil
}
func (self *edges) add(n *node) {
	*self = append(*self, &edge{n.key[0], n})
}
func (self *edges) replace(n *node) {
	for _, e := range *self {
		if e.key == n.key[0] {
			e.node = n
			return
		}
	}
}

type node struct {
	key []byte
	value Hasher
	children *edges
}
func newNode(key []byte, value Hasher) *node {
	return &node{
		key: key,
		value: value,
		children: newEdges(),
	}
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
	for _, edge := range *self.children {
		edge.node.describe(indent + len(self.key), buffer)
	}
}
func (self *node) trimKey(from, to int) {
	new_key := make([]byte, to - from)
	copy(new_key, self.key[from:to])
	self.key = new_key
}
func (self *node) get(key []byte) (value Hasher, existed bool) {
	if current := self.children.get(key[0]); current != nil {
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
	panic("shouldn't happen")
}
func (self *node) insert(node *node) (old Hasher, existed bool) {
	if bytes.Compare(self.key, node.key) == 0 {
		old, self.value, existed = self.value, node.value, true
		return
	} else {
		if current := self.children.get(node.key[0]); current == nil {
			self.children.add(node)
			return
		} else {
			for i := 0;; i ++ {
				if i >= len(node.key) {
					self.children.replace(node)
					current.trimKey(i, len(current.key))
					node.children.add(current)
					return
				} else if i >= len(current.key) {
					node.trimKey(i, len(node.key))
					old, existed = current.insert(node)
					return
				} else if node.key[i] != current.key[i] {
					extra_node := newNode(make([]byte, i), nil)
					copy(extra_node.key, node.key[:i])
					self.children.replace(extra_node)
					node.trimKey(i, len(node.key))
					extra_node.children.add(node)
					current.trimKey(i, len(current.key))
					extra_node.children.add(current)
					return
				}
			}
		}
	}
	panic("shouldn't happen")
}

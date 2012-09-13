package radix

import (
	"../murmur"
	"bytes"
	"fmt"
)

type Hasher interface {
	Hash() []byte
}

type TreeIterator func(key []byte, value Hasher)

const (
	parts = 2
)

func rip(b []byte) (result []byte) {
	result = make([]byte, parts * len(b))
	for i, char := range b {
		for j := 0; j < parts; j++ {
			result[(i*parts)+j] = (char << byte((8/parts)*j)) >> byte(8-(8/parts))
		}
	}
	return
}
func stitch(b []byte) (result []byte) {
	result = make([]byte, len(b) / parts)
	for i, _ := range result {
		for j := 0; j < parts; j++ {
			result[i] += b[(i*parts)+j] << byte((parts-j-1)*(8/parts))
		}
	}
	return
}

type StringHasher string

func (self StringHasher) Hash() []byte {
	return murmur.HashString(string(self))
}

type Tree struct {
	size int
	root *node
}

func (self *Tree) Put(key []byte, value Hasher) (old Hasher, existed bool) {
	self.root, old, existed = self.root.insert(newNode(rip(key), value, true))
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
	self.root, old, existed = self.root.del(rip(key))
	if existed {
		self.size--
	}
	return
}
func (self *Tree) Up(from, below []byte, f TreeIterator) {
//	self.root.up(from, below, f)
}
func (self *Tree) Down(from, above []byte, f TreeIterator) {
//	self.root.down(from, above, f)
}
func (self *Tree) Size() int {
	return self.size
}
func (self *Tree) Describe() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("<Radix size:%v>\n", self.Size()))
	self.root.eachChild(func(node *node) {
		node.describe(2, buffer)
	})
	return string(buffer.Bytes())
}

type node struct {
	key       []byte
	value     Hasher
	hasValue bool
	valueHash []byte
	hash      []byte
	children  []*node
}

func newNode(key []byte, value Hasher, hasValue bool) (result *node) {
	result = &node{
		key:      key,
		value:    value,
		hasValue: hasValue,
		hash:     make([]byte, murmur.Size),
		children: make([]*node, 1<<(8/parts)),
	}
	if value != nil {
		result.valueHash = value.Hash()
	}
	return
}
func (self *node) rehash() {
	h := murmur.NewBytes(self.key)
	h.Write(self.valueHash)
	self.eachChild(func(node *node) {
		h.Write(node.hash)
	})
	h.Extrude(&self.hash)
}
func (self *node) eachChild(f func(child *node)) {
	for _, child := range self.children {
		if child != nil {
			f(child)
		}
	}
}
func (self *node) describe(indent int, buffer *bytes.Buffer) {
	indentation := &bytes.Buffer{}
	for i := 0; i < indent; i++ {
		fmt.Fprint(indentation, " ")
	}
	fmt.Fprintf(buffer, "%v%v", string(indentation.Bytes()), self.key)
	if self.value != nil {
		fmt.Fprintf(buffer, " => %v", self.value)
	}
	fmt.Fprintf(buffer, "\n")
	self.eachChild(func(node *node) {
		node.describe(indent+len(fmt.Sprint(self.key)), buffer)
	})
}
func (self *node) trimKey(from, to int) {
	new_key := make([]byte, to-from)
	copy(new_key, self.key[from:to])
	self.key = new_key
}
func (self *node) get(key []byte) (value Hasher, existed bool) {
	if self == nil {
		return
	}
	beyond_self := false
	beyond_key := false
	for i := 0; ; i++ {
		beyond_self = i >= len(self.key)
		beyond_key = i >= len(key)
		if beyond_self && beyond_key {
			value, existed = self.value, self.hasValue
			return
		} else if beyond_key {
			return
		} else if beyond_self {
			value, existed = self.children[key[i]].get(key[i:])
			return
		} else if key[i] != self.key[i] {
			return
		}
	}
	panic("Shouldn't happen")
}
func (self *node) del(key []byte) (result *node, old Hasher, existed bool) {
	if self == nil {
		return
	}
	beyond_key := false
	beyond_self := false
	for i := 0; ; i++ {
		beyond_key = i >= len(key)
		beyond_self = i >= len(self.key)
		if beyond_key && beyond_self {
			n_children := 0
			var a_child *node
			for _, child := range self.children {
				if child != nil {
					n_children++
					a_child = child
				}
			}
			if n_children > 1 {
				self.hasValue, result, old, existed = false, self, self.value, self.hasValue
			} else if n_children == 1 {
				a_child.key = append(self.key, a_child.key...)
				a_child.rehash()
				result, old, existed = a_child, self.value, self.hasValue
			} else {
				result, old, existed = nil, self.value, self.hasValue
			}
			return
		} else if beyond_key {
			result, old, existed = self, nil, false
			return
		} else if beyond_self {
			self.children[key[0]], old, existed = self.children[key[0]].del(key[i:])
			self.rehash()
			return
		} else if self.key[i] != key[i] {
			return
		}
	}	
	panic("Shouldn't happen")
}
func (self *node) insert(n *node) (result *node, old Hasher, existed bool) {
	if self == nil {
		n.rehash()
		result = n
		return
	}
	beyond_n := false
	beyond_self := false
	for i := 0; ; i++ {
		beyond_n = i >= len(n.key)
		beyond_self = i >= len(self.key)
		if beyond_n && beyond_self {
			self.value, result, old, existed = n.value, self, self.value, true
			self.rehash()
			return
		} else if beyond_n {
			self.trimKey(i, len(self.key))
			n.children[self.key[0]] = self
			result, old, existed = n, nil, false
			self.rehash()
			n.rehash()
			return
		} else if beyond_self {
			n.trimKey(i, len(n.key))
			k := n.key[0]
			self.children[k], old, existed = self.children[k].insert(n)
			self.rehash()
			result = self
			return 
		} else if n.key[i] != self.key[i] {
			result, old, existed = newNode(make([]byte, i), nil, false), nil, false
			copy(result.key, n.key[:i])

			n.trimKey(i, len(n.key))
			result.children[n.key[0]] = n
			
			self.trimKey(i, len(self.key))
			result.children[self.key[0]] = self

			n.rehash()
			self.rehash()
			result.rehash()

			return
		}
	}
	panic("Shouldn't happen")
}


package radix

import (
	"../murmur"
	"bytes"
	"fmt"
)

var nilHash = murmur.HashBytes(nil)

type Hasher interface {
	Hash() []byte
}

type TreeIterator func(key []byte, value Hasher)

const (
	parts = 2
)

func hash(h Hasher) []byte {
	if h == nil {
		return nilHash
	}
	return h.Hash()
}

func rip(b []byte) (result []byte) {
	result = make([]byte, parts*len(b))
	for i, char := range b {
		for j := 0; j < parts; j++ {
			result[(i*parts)+j] = (char << byte((8/parts)*j)) >> byte(8-(8/parts))
		}
	}
	return
}
func stitch(b []byte) (result []byte) {
	result = make([]byte, len(b)/parts)
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

type SubPrint struct {
	Key []byte
	Sum []byte
}

type Print struct {
	Key []byte
	ValueHash []byte
	SubPrints []SubPrint
}
func (self *Print) push(n *node) {
	self.Key = append(self.Key, n.segment...)
}
func (self *Print) set(n *node) {
	self.ValueHash = n.valueHash
	self.SubPrints = make([]SubPrint, len(n.children))
	for index, child := range n.children {
		if child != nil {
			self.SubPrints[index] = SubPrint{
				Key: append(self.Key, child.segment...),
				Sum: child.hash,
			}
		}
	}
}

type Tree struct {
	size int
	root *node
}
func NewTree() *Tree {
	return &Tree{0, newNode(nil, nil, false)}
}

func (self *Tree) Finger(key []byte) (result *Print) {
	result = &Print{}
	self.root.finger(result, key)
	return
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

type node struct {
	segment       []byte
	key []byte
	value     Hasher
	valueHash []byte
	hash      []byte
	children  []*node
}

func newNode(segment []byte, value Hasher, hasValue bool) (result *node) {
	result = &node{
		segment:      segment,
		value:    value,
		hash:     make([]byte, murmur.Size),
		children: make([]*node, 1<<(8/parts)),
	}
	if hasValue {
		result.valueHash = hash(result.value)
	}
	return
}
func (self *node) setSegment(part []byte) {
	new_segment := make([]byte, len(part))
	copy(new_segment, part)
	self.segment = new_segment
}
func (self *node) rehash(key []byte) {
	self.key = key
	h := murmur.NewBytes(key)
	h.Write(self.valueHash)
	self.eachChild(func(node *node) {
		h.Write(node.hash)
	})
	h.Extrude(&self.hash)
}
func (self *node) eachChild(f func(child *node)) {
	if self != nil {
		for _, child := range self.children {
			if child != nil {
				f(child)
			}
		}
	}
}
func (self *node) describe(indent int, buffer *bytes.Buffer) {
	indentation := &bytes.Buffer{}
	for i := 0; i < indent; i++ {
		fmt.Fprint(indentation, " ")
	}
	fmt.Fprintf(buffer, "%v%v", string(indentation.Bytes()), self.segment)
	if self.value != nil {
		fmt.Fprintf(buffer, " => %v", self.value)
	}
	fmt.Fprintf(buffer, " (%v, %v)", self.key, self.hash)
	fmt.Fprintf(buffer, "\n")
	self.eachChild(func(node *node) {
		node.describe(indent+len(fmt.Sprint(self.segment)), buffer)
	})
}
func (self *node) each(prefix []byte, f TreeIterator) {
	if self != nil {
		prefix = append(prefix, self.segment...)
		if self.valueHash != nil {
			f(stitch(prefix), self.value)
		}
		for _, child := range self.children {
			child.each(prefix, f)
		}
	}
}
func (self *node) finger(result *Print, segment []byte) {
	if self == nil {
		return
	}
	result.push(self)
	beyond_self := false
	beyond_segment := false
	for i := 0; ; i++ {
		beyond_self = i >= len(self.segment)
		beyond_segment = i >= len(segment)
		if beyond_self && beyond_segment {
			result.set(self)
			return
		} else if beyond_segment {
			return
		} else if beyond_self {
			self.children[segment[i]].finger(result, segment[i:])
			return
		} else if segment[i] != self.segment[i] {
			return
		}
	}
	panic("Shouldn't happen")
}
func (self *node) get(segment []byte) (value Hasher, existed bool) {
	if self == nil {
		return
	}
	beyond_self := false
	beyond_segment := false
	for i := 0; ; i++ {
		beyond_self = i >= len(self.segment)
		beyond_segment = i >= len(segment)
		if beyond_self && beyond_segment {
			value, existed = self.value, self.valueHash != nil
			return
		} else if beyond_segment {
			return
		} else if beyond_self {
			value, existed = self.children[segment[i]].get(segment[i:])
			return
		} else if segment[i] != self.segment[i] {
			return
		}
	}
	panic("Shouldn't happen")
}
func (self *node) del(prefix, segment []byte) (result *node, old Hasher, existed bool) {
	if self == nil {
		return
	}
	beyond_segment := false
	beyond_self := false
	for i := 0; ; i++ {
		beyond_segment = i >= len(segment)
		beyond_self = i >= len(self.segment)
		if beyond_segment && beyond_self {
			n_children := 0
			var a_child *node
			for _, child := range self.children {
				if child != nil {
					n_children++
					a_child = child
				}
			}
			if n_children > 1 || self.segment == nil {
				result, old, existed = self, self.value, self.valueHash != nil
				self.value, self.valueHash = nil, nil
				self.rehash(append(prefix, segment...))
			} else if n_children == 1 {
				a_child.setSegment(append(self.segment, a_child.segment...))
				result, old, existed = a_child, self.value, self.valueHash != nil
			} else {
				result, old, existed = nil, self.value, self.valueHash != nil
			}
			return
		} else if beyond_segment {
			result, old, existed = self, nil, false
			return
		} else if beyond_self {
			prefix = append(prefix, self.segment...)
			self.children[segment[i]], old, existed = self.children[segment[i]].del(prefix, segment[i:])
			result = self
			self.rehash(prefix)
			return
		} else if self.segment[i] != segment[i] {
			return
		}
	}
	panic("Shouldn't happen")
}
func (self *node) insert(prefix []byte, n *node) (result *node, old Hasher, existed bool) {
	if self == nil {
		n.rehash(append(prefix, n.segment...))
		result = n
		return
	}
	beyond_n := false
	beyond_self := false
	for i := 0; ; i++ {
		beyond_n = i >= len(n.segment)
		beyond_self = i >= len(self.segment)
		if beyond_n && beyond_self {
			result, old, existed = self, self.value, self.valueHash != nil
			self.value, self.valueHash = n.value, hash(n.value)
			self.rehash(append(prefix, self.segment...))
			return
		} else if beyond_n {
			self.setSegment(self.segment[i:])
			n.children[self.segment[0]] = self
			result, old, existed = n, nil, false
			prefix = append(prefix, self.segment...)
			self.rehash(prefix)
			n.rehash(append(prefix, n.segment...))
			return
		} else if beyond_self {
			n.setSegment(n.segment[i:])
			// k is pre-calculated here because n.segment may change when n is inserted
			k := n.segment[0]
			prefix = append(prefix, self.segment...)
			self.children[k], old, existed = self.children[k].insert(prefix, n)
			self.rehash(prefix)
			result = self
			return
		} else if n.segment[i] != self.segment[i] {
			result, old, existed = newNode(nil, nil, false), nil, false
			result.setSegment(n.segment[:i])

			n.setSegment(n.segment[i:])
			result.children[n.segment[0]] = n

			self.setSegment(self.segment[i:])
			result.children[self.segment[0]] = self

			prefix = append(prefix, result.segment...)

			n.rehash(append(prefix, n.segment...))
			self.rehash(append(prefix, self.segment...))
			result.rehash(prefix)

			return
		}
	}
	panic("Shouldn't happen")
}

package radix

import (
	"../murmur"
	"bytes"
	"encoding/hex"
	"fmt"
)

type node struct {
	segment   []Nibble
	value     Hasher
	version   int64
	valueHash []byte
	hash      []byte
	children  []*node
	size      int
}

func newNode(segment []Nibble, value Hasher, version int64, hasValue bool) (result *node) {
	result = &node{
		segment:  segment,
		value:    value,
		version:  version,
		hash:     make([]byte, murmur.Size),
		children: make([]*node, 1<<(8/parts)),
	}
	if hasValue {
		result.valueHash = hash(result.value)
	}
	return
}
func (self *node) setSegment(part []Nibble) {
	new_segment := make([]Nibble, len(part))
	copy(new_segment, part)
	self.segment = new_segment
}
func (self *node) rehash(key []Nibble) {
	self.size = 0
	if self.valueHash != nil {
		self.size++
	}
	h := murmur.NewBytes(toBytes(key))
	h.Write(self.valueHash)
	for _, child := range self.children {
		if child != nil {
			self.size += child.size
			h.Write(child.hash)
		}
	}
	h.Extrude(&self.hash)
}
func (self *node) each(prefix []Nibble, f TreeIterator) (cont bool) {
	cont = true
	if self != nil {
		prefix = append(prefix, self.segment...)
		if self.valueHash != nil {
			cont = f(stitch(prefix), self.value, self.version)
		}
		if cont {
			for _, child := range self.children {
				cont = child.each(prefix, f)
				if !cont {
					break
				}
			}
		}
	}
	return
}
func (self *node) reverseEach(prefix []Nibble, f TreeIterator) (cont bool) {
	cont = true
	if self != nil {
		prefix = append(prefix, self.segment...)
		for i := len(self.children) - 1; i >= 0; i-- {
			cont = self.children[i].reverseEach(prefix, f)
			if !cont {
				break
			}
		}
		if cont {
			if self.valueHash != nil {
				cont = f(stitch(prefix), self.value, self.version)
			}
		}
	}
	return
}
func (self *node) reverseEachBetween(prefix, min, max []Nibble, mincmp, maxcmp int, f TreeIterator) (cont bool) {
	cont = true
	prefix = append(prefix, self.segment...)
	var child *node
	for i := len(self.children) - 1; i >= 0; i-- {
		child = self.children[i]
		if child != nil {
			childKey := make([]Nibble, len(prefix)+len(child.segment))
			copy(childKey, prefix)
			copy(childKey[len(prefix):], child.segment)
			m := len(childKey)
			if m > len(min) {
				m = len(min)
			}
			if m > len(max) {
				m = len(max)
			}
			if (min == nil || nComp(childKey[:m], min[:m]) > -1) && (max == nil || nComp(childKey[:m], max[:m]) < 1) {
				cont = child.reverseEachBetween(prefix, min, max, mincmp, maxcmp, f)
			}
			if !cont {
				break
			}
		}
	}
	if cont {
		if self.valueHash != nil && (min == nil || nComp(prefix, min) > mincmp) && (max == nil || nComp(prefix, max) < maxcmp) {
			cont = f(stitch(prefix), self.value, self.version)
		}
	}
	return
}
func (self *node) eachBetween(prefix, min, max []Nibble, mincmp, maxcmp int, f TreeIterator) (cont bool) {
	cont = true
	prefix = append(prefix, self.segment...)
	if self.valueHash != nil && (min == nil || nComp(prefix, min) > mincmp) && (max == nil || nComp(prefix, max) < maxcmp) {
		cont = f(stitch(prefix), self.value, self.version)
	}
	if cont {
		for _, child := range self.children {
			if child != nil {
				childKey := make([]Nibble, len(prefix)+len(child.segment))
				copy(childKey, prefix)
				copy(childKey[len(prefix):], child.segment)
				m := len(childKey)
				if m > len(min) {
					m = len(min)
				}
				if m > len(max) {
					m = len(max)
				}
				if (min == nil || nComp(childKey[:m], min[:m]) > -1) && (max == nil || nComp(childKey[:m], max[:m]) < 1) {
					cont = child.eachBetween(prefix, min, max, mincmp, maxcmp, f)
				}
				if !cont {
					break
				}
			}
		}
	}
	return
}
func (self *node) eachBetweenIndex(prefix []Nibble, count int, min, max *int, f TreeIterator) (cont bool) {
	cont = true
	prefix = append(prefix, self.segment...)
	if self.valueHash != nil && (min == nil || count >= *min) && (max == nil || count <= *max) {
		cont = f(stitch(prefix), self.value, self.version)
		count++
	}
	if cont {
		for _, child := range self.children {
			if child != nil {
				if (min == nil || child.size+count > *min) && (max == nil || count <= *max) {
					cont = child.eachBetweenIndex(prefix, count, min, max, f)
				}
				count += child.size
				if !cont {
					break
				}
			}
		}
	}
	return
}
func (self *node) describe(indent int, buffer *bytes.Buffer) {
	indentation := &bytes.Buffer{}
	for i := 0; i < indent; i++ {
		fmt.Fprint(indentation, " ")
	}
	encodedSegment := stringEncode(toBytes(self.segment))
	fmt.Fprintf(buffer, "%v%v", string(indentation.Bytes()), encodedSegment)
	if self.valueHash != nil {
		if subTree, ok := self.value.(*Tree); ok {
			fmt.Fprintf(buffer, " => %v", subTree.describeIndented(indent+2))
		} else {
			fmt.Fprintf(buffer, " => %v", self.value)
		}
	}
	fmt.Fprintf(buffer, " (%v, %v, %v)", self.version, self.size, hex.EncodeToString(self.hash))
	fmt.Fprintf(buffer, "\n")
	for _, child := range self.children {
		if child != nil {
			child.describe(indent+len(encodedSegment), buffer)
		}
	}
}
func (self *node) finger(allocated *Print, segment []Nibble) (result *Print) {
	result = allocated
	if self == nil {
		return
	}
	allocated.push(self)
	beyond_self := false
	beyond_segment := false
	for i := 0; ; i++ {
		beyond_self = i >= len(self.segment)
		beyond_segment = i >= len(segment)
		if beyond_self && beyond_segment {
			allocated.set(self)
			return
		} else if beyond_segment {
			return
		} else if beyond_self {
			return self.children[segment[i]].finger(allocated, segment[i:])
		} else if segment[i] != self.segment[i] {
			return
		}
	}
	panic("Shouldn't happen")
}
func (self *node) get(segment []Nibble) (value Hasher, version int64, existed bool) {
	if self == nil {
		return
	}
	beyond_self := false
	beyond_segment := false
	for i := 0; ; i++ {
		beyond_self = i >= len(self.segment)
		beyond_segment = i >= len(segment)
		if beyond_self && beyond_segment {
			value, version, existed = self.value, self.version, self.valueHash != nil
			return
		} else if beyond_segment {
			return
		} else if beyond_self {
			value, version, existed = self.children[segment[i]].get(segment[i:])
			return
		} else if segment[i] != self.segment[i] {
			return
		}
	}
	panic("Shouldn't happen")
}
func (self *node) del(prefix, segment []Nibble) (result *node, old Hasher, existed bool) {
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
				self.value, self.valueHash, self.version = nil, nil, 0
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
			if self.valueHash == nil && prefix != nil {
				n_children := 0
				for _, child := range self.children {
					if child != nil {
						n_children++
					}
				}
				if n_children == 0 {
					result = nil
				} else {
					result = self
					self.rehash(prefix)
				}
			} else {
				result = self
				self.rehash(prefix)
			}
			return
		} else if self.segment[i] != segment[i] {
			return
		}
	}
	panic("Shouldn't happen")
}
func (self *node) insert(prefix []Nibble, n *node) (result *node, old Hasher, version int64, existed bool) {
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
			result, old, version, existed = self, self.value, self.version, self.valueHash != nil
			self.value, self.valueHash = n.value, hash(n.value)
			self.version = n.version
			self.rehash(append(prefix, self.segment...))
			return
		} else if beyond_n {
			self.setSegment(self.segment[i:])
			n.children[self.segment[0]] = self
			result, old, version, existed = n, nil, 0, false
			prefix = append(prefix, self.segment...)
			self.rehash(prefix)
			n.rehash(append(prefix, n.segment...))
			return
		} else if beyond_self {
			n.setSegment(n.segment[i:])
			// k is pre-calculated here because n.segment may change when n is inserted
			k := n.segment[0]
			prefix = append(prefix, self.segment...)
			self.children[k], old, version, existed = self.children[k].insert(prefix, n)
			self.rehash(prefix)
			result = self
			return
		} else if n.segment[i] != self.segment[i] {
			result, old, version, existed = newNode(nil, nil, 0, false), nil, 0, false
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

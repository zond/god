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
	self.eachChild(func(node *node) {
		self.size += node.size
		h.Write(node.hash)
	})
	h.Extrude(&self.hash)
}
func (self *node) each(prefix []Nibble, f TreeIterator) {
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
func (self *node) eachBetween(prefix, min, max []Nibble, mincmp, maxcmp int, f TreeIterator) {
	prefix = append(prefix, self.segment...)
	if self.valueHash != nil && (min == nil || nComp(prefix, min) > mincmp) && (max == nil || nComp(prefix, max) < maxcmp) {
		f(stitch(prefix), self.value)
	}
	for _, child := range self.children {
		if child != nil {
			childKey := make([]Nibble, len(prefix)+len(child.segment))
			copy(childKey, prefix)
			copy(childKey[len(prefix):], child.segment)
			minlen := len(min)
			if minlen > len(childKey) {
				minlen = len(childKey)
			}
			maxlen := len(max)
			if maxlen > len(childKey) {
				maxlen = len(childKey)
			}
			if (min == nil || nComp(childKey, min[:minlen]) > -1) && (max == nil || nComp(childKey, max[:maxlen]) < 1) {
				child.eachBetween(prefix, min, max, mincmp, maxcmp, f)
			}
		}
	}
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
	self.eachChild(func(node *node) {
		node.describe(indent+len(encodedSegment), buffer)
	})
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
func (self *node) index(prefix []Nibble, n int, up bool) (nibble []Nibble, value Hasher, version int64, existed bool) {
	if n == 0 {
		if self.valueHash != nil {
			nibble, value, version, existed = append(prefix, self.segment...), self.value, self.version, true
			return
		}
		if up {
			return self.nextChild(prefix, nil)
		} else {
			return self.prevChild(prefix, nil)
		}
	}
	if self.valueHash != nil {
		n--
	}
	if self.size < n {
		return
	}
	start, step, end := 0, 1, len(self.children)
	if !up {
		start, step, end = len(self.children)-1, -1, -1
	}
	var child *node
	for i := start; i != end; i += step {
		child = self.children[i]
		if child != nil {
			if child.size <= n {
				n -= child.size
			} else {
				return child.index(append(prefix, self.segment...), n, up)
			}
		}
	}
	panic("Shouldn't happen")
}
func (self *node) first(prefix []Nibble) (nibble []Nibble, value Hasher, version int64, existed bool) {
	if self == nil {
		return
	}
	prefix = append(prefix, self.segment...)
	for _, child := range self.children {
		if child != nil {
			if nibble, value, version, existed = child.first(prefix); existed {
				return
			}
		}
	}
	nibble, value, version, existed = prefix, self.value, self.version, self.valueHash != nil
	return
}
func (self *node) last(prefix []Nibble) (nibble []Nibble, value Hasher, version int64, existed bool) {
	if self == nil {
		return
	}
	prefix = append(prefix, self.segment...)
	for i := len(self.children) - 1; i >= 0; i-- {
		child := self.children[i]
		if child != nil {
			if nibble, value, version, existed = child.last(prefix); existed {
				return
			}
		}
	}
	nibble, value, version, existed = prefix, self.value, self.version, self.valueHash != nil
	return
}
func (self *node) nextChild(prefix, segment []Nibble) (nextNibble []Nibble, nextValue Hasher, nextVersion int64, existed bool) {
	recursePrefix := make([]Nibble, len(prefix)+len(self.segment))
	copy(recursePrefix, prefix)
	copy(recursePrefix[len(prefix):], self.segment)
	var firstChild int
	var restSegment []Nibble
	if len(segment) > len(self.segment) {
		restSegment = segment[len(self.segment):]
		firstChild = int(restSegment[0])
	}
	var child *node
	for i := firstChild; i < len(self.children); i++ {
		child = self.children[i]
		if nextNibble, nextValue, nextVersion, existed = child.next(recursePrefix, restSegment); existed {
			return
		}
	}
	return
}
func (self *node) next(prefix, segment []Nibble) (nextNibble []Nibble, nextValue Hasher, nextVersion int64, existed bool) {
	if self == nil {
		return
	}
	beyond_segment := false
	beyond_self := false
	for i := 0; ; i++ {
		beyond_segment = i >= len(segment)
		beyond_self = i >= len(self.segment)
		if beyond_segment && beyond_self {
			return self.nextChild(prefix, nil)
		} else if beyond_segment {
			if self.valueHash != nil {
				nextNibble, nextValue, nextVersion, existed = append(prefix, self.segment...), self.value, self.version, true
				return
			} else {
				return self.nextChild(prefix, nil)
			}
		} else if beyond_self {
			return self.nextChild(prefix, segment)
		} else if segment[i] != self.segment[i] {
			if segment[i] > self.segment[i] {
				return
			}
			if self.valueHash != nil {
				nextNibble, nextValue, nextVersion, existed = append(prefix, self.segment...), self.value, self.version, true
				return
			}
			return self.nextChild(prefix, nil)
		}
	}
	panic("Shouldn't happen")
}
func (self *node) prevChild(prefix, segment []Nibble) (prevNibble []Nibble, prevValue Hasher, prevVersion int64, existed bool) {
	recursePrefix := make([]Nibble, len(prefix)+len(self.segment))
	copy(recursePrefix, prefix)
	copy(recursePrefix[len(prefix):], self.segment)
	lastChild := len(self.children) - 1
	var restSegment []Nibble
	if len(segment) > len(self.segment) {
		restSegment = segment[len(self.segment):]
		lastChild = int(restSegment[0])
	}
	var child *node
	for i := lastChild; i >= 0; i-- {
		child = self.children[i]
		if prevNibble, prevValue, prevVersion, existed = child.prev(recursePrefix, restSegment); existed {
			return
		}
	}
	return
}
func (self *node) prev(prefix, segment []Nibble) (prevNibble []Nibble, prevValue Hasher, prevVersion int64, existed bool) {
	if self == nil {
		return
	}
	if segment == nil {
		if self.valueHash != nil {
			prevNibble, prevValue, prevVersion, existed = append(prefix, self.segment...), self.value, self.version, self.valueHash != nil
			return
		}
		return self.prevChild(append(prefix, self.segment...), nil)
	}
	beyond_segment := false
	beyond_self := false
	for i := 0; ; i++ {
		beyond_segment = i >= len(segment)
		beyond_self = i >= len(self.segment)
		if beyond_segment && beyond_self {
			return
		} else if beyond_segment {
			return
		} else if beyond_self {
			if prevNibble, prevValue, prevVersion, existed = self.prevChild(prefix, segment); existed {
				return
			}
			prevNibble, prevValue, prevVersion, existed = append(prefix, self.segment...), self.value, self.version, self.valueHash != nil
			return
		} else if segment[i] != self.segment[i] {
			if segment[i] <= self.segment[i] {
				return
			}
			if self.valueHash != nil {
				prevNibble, prevValue, prevVersion, existed = append(prefix, self.segment...), self.value, self.version, self.valueHash != nil
				return
			}
			return self.prevChild(prefix, nil)
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

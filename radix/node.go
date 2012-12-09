package radix

import (
	"../murmur"
	"bytes"
	"encoding/hex"
	"fmt"
	"strings"
)

const (
	byteValue = 1 << iota
	treeValue
)

type nodeIndexIterator func(key, byteValue []byte, treeValue *Tree, use int, timestamp int64, index int) (cont bool)

type nodeIterator func(key, byteValue []byte, treeValue *Tree, use int, timestamp int64) (cont bool)

// node is the generic implementation of a combined radix/merkle tree with size for each subtree (both regarding bytes and inner trees) cached.
// it also contains both byte slices and inner trees in each node.
type node struct {
	segment   []Nibble
	byteValue []byte
	byteHash  []byte
	treeValue *Tree
	timestamp int64
	hash      []byte
	children  []*node
	empty     bool // this node only serves a structural purpose (ie remove it if it is no longer useful for that)
	use       int  // the values in this node that are to be considered 'present'. even if this is a zero, do not remove the node if empty is false - it is still a delete marker.
	treeSize  int
	byteSize  int
}

func newNode(segment []Nibble, byteValue []byte, treeValue *Tree, timestamp int64, empty bool, use int) *node {
	return &node{
		segment:   segment,
		byteValue: byteValue,
		byteHash:  murmur.HashBytes(byteValue),
		treeValue: treeValue,
		timestamp: timestamp,
		hash:      make([]byte, murmur.Size),
		children:  make([]*node, 1<<(8/parts)),
		empty:     empty,
		use:       use,
	}
}
func (self *node) setSegment(part []Nibble) {
	new_segment := make([]Nibble, len(part))
	copy(new_segment, part)
	self.segment = new_segment
}
func (self *node) rehash(key []Nibble) {
	self.treeSize = 0
	self.byteSize = 0
	if self.use&treeValue != 0 {
		self.treeSize = self.treeValue.Size()
	}
	if self.use&byteValue != 0 {
		self.byteSize = 1
	}
	h := murmur.NewBytes(toBytes(key))
	h.Write(self.byteHash)
	h.Write(self.treeValue.Hash())
	for _, child := range self.children {
		if child != nil {
			self.treeSize += child.treeSize
			self.byteSize += child.byteSize
			h.Write(child.hash)
		}
	}
	h.Extrude(&self.hash)
}
func (self *node) each(prefix []Nibble, use int, f nodeIterator) (cont bool) {
	cont = true
	if self != nil {
		prefix = append(prefix, self.segment...)
		if self.use&use != 0 {
			cont = f(Stitch(prefix), self.byteValue, self.treeValue, self.use, self.timestamp)
		}
		if cont {
			for _, child := range self.children {
				cont = child.each(prefix, use, f)
				if !cont {
					break
				}
			}
		}
	}
	return
}
func (self *node) reverseEach(prefix []Nibble, use int, f nodeIterator) (cont bool) {
	cont = true
	if self != nil {
		prefix = append(prefix, self.segment...)
		for i := len(self.children) - 1; i >= 0; i-- {
			cont = self.children[i].reverseEach(prefix, use, f)
			if !cont {
				break
			}
		}
		if cont {
			if self.use&use != 0 {
				cont = f(Stitch(prefix), self.byteValue, self.treeValue, self.use, self.timestamp)
			}
		}
	}
	return
}
func (self *node) eachBetween(prefix, min, max []Nibble, mincmp, maxcmp, use int, f nodeIterator) (cont bool) {
	cont = true
	prefix = append(prefix, self.segment...)
	if self.use&use != 0 && (min == nil || nComp(prefix, min) > mincmp) && (max == nil || nComp(prefix, max) < maxcmp) {
		cont = f(Stitch(prefix), self.byteValue, self.treeValue, self.use, self.timestamp)
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
					cont = child.eachBetween(prefix, min, max, mincmp, maxcmp, use, f)
				}
				if !cont {
					break
				}
			}
		}
	}
	return
}
func (self *node) reverseEachBetween(prefix, min, max []Nibble, mincmp, maxcmp, use int, f nodeIterator) (cont bool) {
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
				cont = child.reverseEachBetween(prefix, min, max, mincmp, maxcmp, use, f)
			}
			if !cont {
				break
			}
		}
	}
	if cont {
		if self.use&use != 0 && (min == nil || nComp(prefix, min) > mincmp) && (max == nil || nComp(prefix, max) < maxcmp) {
			cont = f(Stitch(prefix), self.byteValue, self.treeValue, self.use, self.timestamp)
		}
	}
	return
}
func (self *node) sizeBetween(prefix, min, max []Nibble, mincmp, maxcmp, use int) (result int) {
	prefix = append(prefix, self.segment...)
	m := len(prefix)
	if m > len(min) {
		m = len(min)
	}
	if m > len(max) {
		m = len(max)
	}
	if (min == nil || nComp(prefix[:m], min[:m]) > 0) && (max == nil || nComp(prefix[:m], max[:m]) < 0) {
		if use&byteValue != 0 {
			result += self.byteSize
		}
		if use&treeValue != 0 {
			result += self.treeSize
		}
		return
	}
	if self.use&use != 0 && (min == nil || nComp(prefix, min) > mincmp) && (max == nil || nComp(prefix, max) < maxcmp) {
		if self.use&use&byteValue != 0 {
			result++
		}
		if self.use&use&treeValue != 0 {
			result += self.treeValue.Size()
		}
	}
	for _, child := range self.children {
		if child != nil {
			result += child.sizeBetween(prefix, min, max, mincmp, maxcmp, use)
		}
	}
	return
}
func (self *node) eachBetweenIndex(prefix []Nibble, count int, min, max *int, use int, f nodeIndexIterator) (cont bool) {
	cont = true
	prefix = append(prefix, self.segment...)
	if self.use&use != 0 && (min == nil || count >= *min) && (max == nil || count <= *max) {
		cont = f(Stitch(prefix), self.byteValue, self.treeValue, self.use, self.timestamp, count)
		if self.use&use&byteValue != 0 {
			count++
		}
		if self.use&use&treeValue != 0 {
			count += self.treeValue.Size()
		}
	}
	if cont {
		relevantChildSize := 0
		for _, child := range self.children {
			if child != nil {
				relevantChildSize = 0
				if use&byteValue != 0 {
					relevantChildSize += child.byteSize
				}
				if use&treeValue != 0 {
					relevantChildSize += child.treeSize
				}
				if (min == nil || relevantChildSize+count > *min) && (max == nil || count <= *max) {
					cont = child.eachBetweenIndex(prefix, count, min, max, use, f)
				}
				count += relevantChildSize
				if !cont {
					break
				}
			}
		}
	}
	return
}
func (self *node) reverseEachBetweenIndex(prefix []Nibble, count int, min, max *int, use int, f nodeIndexIterator) (cont bool) {
	cont = true
	prefix = append(prefix, self.segment...)
	var child *node
	relevantChildSize := 0
	for i := len(self.children) - 1; i >= 0; i-- {
		child = self.children[i]
		if child != nil {
			relevantChildSize = 0
			if use&byteValue != 0 {
				relevantChildSize += child.byteSize
			}
			if use&treeValue != 0 {
				relevantChildSize += child.treeSize
			}
			if (min == nil || relevantChildSize+count > *min) && (max == nil || count <= *max) {
				cont = child.reverseEachBetweenIndex(prefix, count, min, max, use, f)
			}
			count += relevantChildSize
			if !cont {
				break
			}
		}
	}
	if cont {
		if self.use&use != 0 && (min == nil || count >= *min) && (max == nil || count <= *max) {
			cont = f(Stitch(prefix), self.byteValue, self.treeValue, self.use, self.timestamp, count)
			if self.use&use&byteValue != 0 {
				count++
			}
			if self.use&use&treeValue != 0 {
				count += self.treeValue.Size()
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
	keyHeader := fmt.Sprintf("%v%#v (%v/%v, %v, %v, %v, %v) => ", string(indentation.Bytes()), encodedSegment, self.byteSize, self.treeSize, self.empty, self.use, self.timestamp, hex.EncodeToString(self.hash))
	if self.empty {
		fmt.Fprintf(buffer, "%v\n", keyHeader)
	} else {
		fmt.Fprintf(buffer, "%v%v\n", keyHeader, strings.Trim(self.treeValue.describeIndented(0, len(keyHeader)), "\n"))
		fmt.Fprintf(buffer, "%v%v\n", keyHeader, self.byteValue)
	}
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
func (self *node) indexOf(count int, segment []Nibble, use int, up bool) (index int, existed int) {
	beyond_self := false
	beyond_segment := false
	for i := 0; ; i++ {
		beyond_self = i >= len(self.segment)
		beyond_segment = i >= len(segment)
		if beyond_self && beyond_segment {
			index, existed = count, self.use
			return
		} else if beyond_segment {
			return
		} else if beyond_self {
			if use&byteValue&self.use != 0 {
				count++
			}
			if use&treeValue&self.use != 0 {
				count += self.treeValue.Size()
			}
			start, step, stop := 0, 1, len(self.children)
			if !up {
				start, step, stop = len(self.children)-1, -1, -1
			}
			var child *node
			for j := start; j != stop; j += step {
				child = self.children[j]
				if child != nil {
					if (up && j < int(segment[i])) || (!up && j > int(segment[i])) {
						if use&byteValue != 0 {
							count += child.byteSize
						}
						if use&treeValue != 0 {
							count += child.treeSize
						}
					} else {
						index, existed = child.indexOf(count, segment[i:], use, up)
						return
					}
				}
			}
			index, existed = count, 0
			return
		} else if segment[i] != self.segment[i] {
			if up {
				if segment[i] < self.segment[i] {
					index, existed = count, 0
				} else {
					index, existed = count+1, 0
				}
			} else {
				if segment[i] > self.segment[i] {
					index, existed = count, 0
				} else {
					for _, child := range self.children {
						if child != nil {
							if use&byteValue != 0 {
								count += child.byteSize
							}
							if use&treeValue != 0 {
								count += child.treeSize
							}
						}
					}
					index, existed = count, 0
				}
			}
			return
		}
	}
	panic("Shouldn't happen")
}
func (self *node) get(segment []Nibble) (byteValue []byte, treeValue *Tree, timestamp int64, existed int) {
	if self == nil {
		return
	}
	beyond_self := false
	beyond_segment := false
	for i := 0; ; i++ {
		beyond_self = i >= len(self.segment)
		beyond_segment = i >= len(segment)
		if beyond_self && beyond_segment {
			byteValue, treeValue, timestamp, existed = self.byteValue, self.treeValue, self.timestamp, self.use
			return
		} else if beyond_segment {
			return
		} else if beyond_self {
			byteValue, treeValue, timestamp, existed = self.children[segment[i]].get(segment[i:])
			return
		} else if segment[i] != self.segment[i] {
			return
		}
	}
	panic("Shouldn't happen")
}
func (self *node) del(prefix, segment []Nibble, use int) (result *node, oldBytes []byte, oldTree *Tree, existed int) {
	if self == nil {
		return
	}
	beyond_segment := false
	beyond_self := false
	for i := 0; ; i++ {
		beyond_segment = i >= len(segment)
		beyond_self = i >= len(self.segment)
		if beyond_segment && beyond_self {
			if self.use&^use != 0 {
				if self.use&use&byteValue != 0 {
					oldBytes = self.byteValue
					existed |= byteValue
					self.byteValue, self.byteHash, self.use = nil, murmur.HashBytes(nil), self.use&^byteValue
				}
				if self.use&use&treeValue != 0 {
					oldTree = self.treeValue
					existed |= treeValue
					self.treeValue, self.use = nil, self.use&^treeValue
				}
				result = self
				self.rehash(append(prefix, segment...))
			} else {
				n_children := 0
				var a_child *node
				for _, child := range self.children {
					if child != nil {
						n_children++
						a_child = child
					}
				}
				if n_children > 1 || self.segment == nil {
					result, oldBytes, oldTree, existed = self, self.byteValue, self.treeValue, self.use
					self.byteValue, self.byteHash, self.treeValue, self.empty, self.use, self.timestamp = nil, murmur.HashBytes(nil), nil, true, 0, 0
					self.rehash(append(prefix, segment...))
				} else if n_children == 1 {
					a_child.setSegment(append(self.segment, a_child.segment...))
					result, oldBytes, oldTree, existed = a_child, self.byteValue, self.treeValue, self.use
				} else {
					result, oldBytes, oldTree, existed = nil, self.byteValue, self.treeValue, self.use
				}
			}
			return
		} else if beyond_segment {
			result, oldBytes, oldTree, existed = self, nil, nil, 0
			return
		} else if beyond_self {
			prefix = append(prefix, self.segment...)
			self.children[segment[i]], oldBytes, oldTree, existed = self.children[segment[i]].del(prefix, segment[i:], use)
			if self.empty && prefix != nil {
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
func (self *node) insert(prefix []Nibble, n *node, use, clear int) (result *node, oldBytes []byte, oldTree *Tree, timestamp int64, existed int) {
	if self == nil {
		if clear&byteValue != 0 {
			n.use &^= byteValue
		}
		if clear&treeValue != 0 {
			n.use &^= byteValue
		}
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
			result, oldBytes, oldTree, timestamp, existed = self, self.byteValue, self.treeValue, self.timestamp, self.use
			if use&byteValue != 0 {
				self.byteValue, self.byteHash = n.byteValue, n.byteHash
				if n.use&byteValue == 0 {
					self.use &^= byteValue
				} else {
					self.use |= byteValue
				}
			}
			if use&treeValue != 0 {
				self.treeValue = n.treeValue
				if n.use&treeValue == 0 {
					self.use &^= treeValue
				} else {
					self.use |= treeValue
				}
			}
			if clear&byteValue != 0 {
				self.use &^= byteValue
			}
			if clear&treeValue != 0 {
				self.use &^= byteValue
			}
			self.empty, self.timestamp = n.empty, n.timestamp
			self.rehash(append(prefix, self.segment...))
			return
		} else if beyond_n {
			self.setSegment(self.segment[i:])
			n.children[self.segment[0]] = self
			result, oldBytes, oldTree, timestamp, existed = n, nil, nil, 0, 0
			prefix = append(prefix, self.segment...)
			self.rehash(prefix)
			if clear&byteValue != 0 {
				n.use &^= byteValue
			}
			if clear&treeValue != 0 {
				n.use &^= byteValue
			}
			n.rehash(append(prefix, n.segment...))
			return
		} else if beyond_self {
			n.setSegment(n.segment[i:])
			// k is pre-calculated here because n.segment may change when n is inserted
			k := n.segment[0]
			prefix = append(prefix, self.segment...)
			self.children[k], oldBytes, oldTree, timestamp, existed = self.children[k].insert(prefix, n, use, clear)
			self.rehash(prefix)
			result = self
			return
		} else if n.segment[i] != self.segment[i] {
			result, oldBytes, oldTree, timestamp, existed = newNode(nil, nil, nil, 0, true, 0), nil, nil, 0, 0
			result.setSegment(n.segment[:i])

			n.setSegment(n.segment[i:])
			result.children[n.segment[0]] = n

			self.setSegment(self.segment[i:])
			result.children[self.segment[0]] = self

			prefix = append(prefix, result.segment...)

			if clear&byteValue != 0 {
				n.use &^= byteValue
			}
			if clear&treeValue != 0 {
				n.use &^= byteValue
			}
			n.rehash(append(prefix, n.segment...))
			self.rehash(append(prefix, self.segment...))
			result.rehash(prefix)

			return
		}
	}
	panic("Shouldn't happen")
}

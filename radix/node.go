package radix

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"github.com/zond/god/murmur"
	"strings"
	"time"
)

const (
	byteValue = 1 << iota
	treeValue
)

const (
	zombieLifetime = int64(time.Hour * 24)
)

type nodeIndexIterator func(key, byteValue []byte, treeValue *Tree, use int, timestamp int64, index int) (cont bool)

type nodeIterator func(key, byteValue []byte, treeValue *Tree, use int, timestamp int64) (cont bool)

// node is the generic implementation of a combined radix/merkle tree with size for each subtree (both regarding bytes and inner trees) cached.
// it also contains both byte slices and inner trees in each node.
type node struct {
	segment   []Nibble // the bit of the key for this node that separates it from its parent
	byteValue []byte
	byteHash  []byte // cached hash of the byteValue
	treeValue *Tree
	timestamp int64  // only used in regard to byteValues. treeValues ignore them (since they have their own timestamps inside them). a timestamp of 0 will be considered REALLY empty
	hash      []byte // cached hash of the entire node
	children  []*node
	empty     bool // this node only serves a structural purpose (ie remove it if it is no longer useful for that)
	use       int  // the values in this node that are to be considered 'present'. even if this is a zero, do not remove the node if empty is false - it is still a delete marker.
	treeSize  int  // size of the tree in this node and those of all of its children
	byteSize  int  // number of byte values in this node and all of its children
	realSize  int  // number of actual values, including delete markers
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

// setSegment copies the given part to be our segment.
func (self *node) setSegment(part []Nibble) {
	new_segment := make([]Nibble, len(part))
	copy(new_segment, part)
	self.segment = new_segment
}

// rehash will recount the size of this node by summing the sizes of its own data and
// the data of its children.
//
// It will also rehash the hash of this node by recalculating the hash sum of its own
// data and the hashes of its children.
//
// Finally it will remove any children that are timed out tombstones.
func (self *node) rehash(key []Nibble, now int64) {
	self.treeSize = 0
	self.byteSize = 0
	self.realSize = 0
	self.realSize += self.treeValue.RealSize()
	if self.timestamp != 0 {
		self.realSize++
	}
	if self.use&treeValue != 0 {
		self.treeSize = self.treeValue.Size()
	}
	if self.use&byteValue != 0 {
		self.byteSize = 1
	}
	h := murmur.NewBytes(toBytes(key))
	h.Write(self.byteHash)
	h.Write(self.treeValue.Hash())
	for index, child := range self.children {
		if child != nil {
			if !child.empty && child.use == 0 && child.timestamp < now-zombieLifetime {
				self.children[index], _, _, _, _ = child.del(key, child.segment, 0, now)
			}
			self.treeSize += child.treeSize
			self.byteSize += child.byteSize
			self.realSize += child.realSize
			h.Write(child.hash)
		}
	}
	h.Extrude(&self.hash)
}
func (self *node) describe(indent int, buffer *bytes.Buffer) {
	if self == nil {
		return
	}
	indentation := &bytes.Buffer{}
	for i := 0; i < indent; i++ {
		fmt.Fprint(indentation, " ")
	}
	encodedSegment := stringEncode(toBytes(self.segment))
	keyHeader := fmt.Sprintf("%v%#v (%v/%v/%v, %v, %v, %v, %v) => ", string(indentation.Bytes()), encodedSegment, self.byteSize, self.treeSize, self.realSize, self.empty, self.use, self.timestamp, hex.EncodeToString(self.hash))
	if self.empty {
		fmt.Fprintf(buffer, "%v\n", keyHeader)
	} else {
		fmt.Fprintf(buffer, "%v%v\n", keyHeader, strings.Trim(self.treeValue.describeIndented(0, len(keyHeader)), "\n"))
		fmt.Fprintf(buffer, "%v%v\n", keyHeader, self.byteValue)
	}
	for _, child := range self.children {
		child.describe(indent+len(encodedSegment), buffer)
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
			if !self.empty {
				if use == 0 || use&byteValue&self.use != 0 {
					count++
				}
				if use == 0 || use&treeValue&self.use != 0 {
					count += self.treeValue.Size()
				}
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
						if use == 0 {
							count += child.realSize
						} else {
							if use&byteValue != 0 {
								count += child.byteSize
							}
							if use&treeValue != 0 {
								count += child.treeSize
							}
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
							if use == 0 {
								count += child.realSize
							} else {
								if use&byteValue != 0 {
									count += child.byteSize
								}
								if use&treeValue != 0 {
									count += child.treeSize
								}
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
func (self *node) del(prefix, segment []Nibble, use int, now int64) (result *node, oldBytes []byte, oldTree *Tree, timestamp int64, existed int) {
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
				result, timestamp = self, self.timestamp
				self.rehash(append(prefix, segment...), now)
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
					result, oldBytes, oldTree, timestamp, existed = self, self.byteValue, self.treeValue, self.timestamp, self.use
					self.byteValue, self.byteHash, self.treeValue, self.empty, self.use, self.timestamp = nil, murmur.HashBytes(nil), nil, true, 0, 0
					self.rehash(append(prefix, segment...), now)
				} else if n_children == 1 {
					a_child.setSegment(append(self.segment, a_child.segment...))
					result, oldBytes, oldTree, timestamp, existed = a_child, self.byteValue, self.treeValue, self.timestamp, self.use
				} else {
					result, oldBytes, oldTree, timestamp, existed = nil, self.byteValue, self.treeValue, self.timestamp, self.use
				}
			}
			return
		} else if beyond_segment {
			result, oldBytes, oldTree, timestamp, existed = self, nil, nil, 0, 0
			return
		} else if beyond_self {
			prefix = append(prefix, self.segment...)
			self.children[segment[i]], oldBytes, oldTree, timestamp, existed = self.children[segment[i]].del(prefix, segment[i:], use, now)
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
					self.rehash(prefix, now)
				}
			} else {
				result = self
				self.rehash(prefix, now)
			}
			return
		} else if self.segment[i] != segment[i] {
			result, oldBytes, oldTree, timestamp, existed = self, nil, nil, 0, 0
			return
		}
	}
	panic("Shouldn't happen")
}
func (self *node) fakeClear(prefix []Nibble, use int, timestamp, now int64) (removed int) {
	if self == nil {
		return
	}
	newPrefix := make([]Nibble, len(prefix)+len(self.segment))
	copy(newPrefix, prefix)
	copy(newPrefix[len(prefix):], self.segment)
	for _, child := range self.children {
		removed += child.fakeClear(newPrefix, use, timestamp, now)
	}
	if !self.empty {
		if self.use&use&byteValue != 0 {
			removed++
			self.byteValue, self.byteHash = nil, murmur.HashBytes(nil)
			self.use &^= byteValue
		}
		if self.use&use&treeValue != 0 {
			removed += self.treeValue.Size()
			self.treeValue = nil
			self.use &^= treeValue
		}
		self.timestamp = timestamp
	}
	self.rehash(newPrefix, now)
	return
}
func (self *node) fakeDel(prefix, segment []Nibble, use int, timestamp, now int64) (result *node, oldBytes []byte, oldTree *Tree, oldTimestamp int64, existed int) {
	return self.insertHelp(prefix, newNode(segment, nil, nil, timestamp, false, 0), use, now)
}
func (self *node) insert(prefix []Nibble, n *node, now int64) (result *node, oldBytes []byte, oldTree *Tree, timestamp int64, existed int) {
	return self.insertHelp(prefix, n, n.use, now)
}
func (self *node) insertHelp(prefix []Nibble, n *node, use int, now int64) (result *node, oldBytes []byte, oldTree *Tree, timestamp int64, existed int) {
	if self == nil {
		n.rehash(append(prefix, n.segment...), now)
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
			self.empty, self.timestamp = n.empty, n.timestamp
			self.rehash(append(prefix, self.segment...), now)
			return
		} else if beyond_n {
			self.setSegment(self.segment[i:])
			n.children[self.segment[0]] = self
			result, oldBytes, oldTree, timestamp, existed = n, nil, nil, 0, 0
			prefix = append(prefix, self.segment...)
			self.rehash(prefix, now)
			n.rehash(append(prefix, n.segment...), now)
			return
		} else if beyond_self {
			n.setSegment(n.segment[i:])
			// k is pre-calculated here because n.segment may change when n is inserted
			k := n.segment[0]
			prefix = append(prefix, self.segment...)
			self.children[k], oldBytes, oldTree, timestamp, existed = self.children[k].insertHelp(prefix, n, use, now)
			self.rehash(prefix, now)
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

			n.rehash(append(prefix, n.segment...), now)
			self.rehash(append(prefix, self.segment...), now)
			result.rehash(prefix, now)

			return
		}
	}
	panic("Shouldn't happen")
}

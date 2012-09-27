package radix

import (
	"../murmur"
	"bytes"
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

func (self *SubPrint) match(other *SubPrint) bool {
	if self == nil {
		return other == nil
	}
	return other != nil && bytes.Compare(other.Sum, self.Sum) == 0
}

type Print struct {
	Key       []byte
	ValueHash []byte
	SubPrints []*SubPrint
}

func (self *Print) match(other *Print) bool {
	if self == nil {
		return other == nil
	}
	return other != nil && bytes.Compare(other.ValueHash, self.ValueHash) == 0
}
func (self *Print) push(n *node) {
	self.Key = append(self.Key, n.segment...)
}
func (self *Print) set(n *node) {
	self.ValueHash = n.valueHash
	self.SubPrints = make([]*SubPrint, len(n.children))
	for index, child := range n.children {
		if child != nil {
			self.SubPrints[index] = &SubPrint{
				Key: append(append([]byte{}, self.Key...), child.segment...),
				Sum: child.hash,
			}
		}
	}
}

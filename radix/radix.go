
package radix

import (
	"../murmur"
)

var nilHash = murmur.HashBytes(nil)

type HashTree interface {
	Hash() []byte
	Finger(key []byte) *Print
	Put(key []byte, value Hasher) (old Hasher, existed bool)
	Get(key []byte) (value Hasher, existed bool)
	Del(key []byte) (old Hasher, existed bool)
}

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
	SubPrints []*SubPrint
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


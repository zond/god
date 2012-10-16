package radix

import (
	"../murmur"
	"bytes"
	"fmt"
	"unsafe"
)

var nilHash = murmur.HashBytes(nil)
var encodeChars = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"}

type Hasher interface {
	Hash() []byte
}

type TreeIterator func(key []byte, value Hasher)
type nibble byte

func toBytes(n []nibble) []byte {
	return *((*[]byte)(unsafe.Pointer(&n)))
}

const (
	parts = 2
)

func hash(h Hasher) []byte {
	if h == nil {
		return nilHash
	}
	return h.Hash()
}

func rip(b []byte) (result []nibble) {
	result = make([]nibble, parts*len(b))
	for i, char := range b {
		for j := 0; j < parts; j++ {
			result[(i*parts)+j] = nibble((char << byte((8/parts)*j)) >> byte(8-(8/parts)))
		}
	}
	return
}
func stringEncode(b []byte) string {
	buffer := new(bytes.Buffer)
	for _, c := range b {
		fmt.Fprint(buffer, encodeChars[c])
	}
	return string(buffer.Bytes())
}
func stitch(b []nibble) (result []byte) {
	result = make([]byte, len(b)/parts)
	for i, _ := range result {
		for j := 0; j < parts; j++ {
			result[i] += byte(b[(i*parts)+j] << byte((parts-j-1)*(8/parts)))
		}
	}
	return
}

type StringHasher string

func (self StringHasher) Hash() []byte {
	return murmur.HashString(string(self))
}

type SubPrint struct {
	Key []nibble
	Sum []byte
}

func (self *SubPrint) equals(other *SubPrint) bool {
	if self == nil {
		return other == nil
	}
	return other != nil && bytes.Compare(other.Sum, self.Sum) == 0
}

type Print struct {
	Key       []nibble
	ValueHash []byte
	Version   int64
	SubTree   bool
	SubPrints []*SubPrint
}

func (self *Print) coveredBy(other *Print) bool {
	if self == nil {
		return other == nil
	}
	return other != nil && (other.Version > self.Version || bytes.Compare(other.ValueHash, self.ValueHash) == 0)
}
func (self *Print) push(n *node) {
	self.Key = append(self.Key, n.segment...)
}
func (self *Print) set(n *node) {
	self.ValueHash = n.valueHash
	self.Version = n.version
	_, self.SubTree = n.value.(*Tree)
	self.SubPrints = make([]*SubPrint, len(n.children))
	for index, child := range n.children {
		if child != nil {
			self.SubPrints[index] = &SubPrint{
				Key: append(append([]nibble{}, self.Key...), child.segment...),
				Sum: child.hash,
			}
		}
	}
}
func (self *Print) version() int64 {
	if self == nil {
		return 0
	}
	return self.Version
}

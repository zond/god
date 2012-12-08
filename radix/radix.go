package radix

import (
	"bytes"
	"fmt"
	"unsafe"
)

var encodeChars = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"}

type Nibble byte

func toBytes(n []Nibble) []byte {
	return *((*[]byte)(unsafe.Pointer(&n)))
}

const (
	parts = 2
)

func nComp(a, b []Nibble) int {
	return bytes.Compare(toBytes(a), toBytes(b))
}

func Rip(b []byte) (result []Nibble) {
	if b == nil {
		return nil
	}
	result = make([]Nibble, parts*len(b))
	for i, char := range b {
		for j := 0; j < parts; j++ {
			result[(i*parts)+j] = Nibble((char << byte((8/parts)*j)) >> byte(8-(8/parts)))
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
func Stitch(b []Nibble) (result []byte) {
	if b == nil {
		return nil
	}
	result = make([]byte, len(b)/parts)
	for i, _ := range result {
		for j := 0; j < parts; j++ {
			result[i] += byte(b[(i*parts)+j] << byte((parts-j-1)*(8/parts)))
		}
	}
	return
}

type SubPrint struct {
	Key    []Nibble
	Sum    []byte
	Exists bool
}

func (self SubPrint) equals(other SubPrint) bool {
	return bytes.Compare(other.Sum, self.Sum) == 0
}

type Print struct {
	Exists    bool
	Key       []Nibble
	Empty     bool
	Version   int64
	SubTree   bool
	SubPrints []SubPrint
	Sum       []byte
}

func (self *Print) coveredBy(other *Print) bool {
	if self == nil {
		return other == nil
	}
	return other != nil && (other.Version > self.Version || bytes.Compare(self.Sum, other.Sum) == 0)
}
func (self *Print) push(n *node) {
	self.Key = append(self.Key, n.segment...)
}
func (self *Print) set(n *node) {
	self.Exists = true
	self.Sum = n.hash
	self.Empty = n.empty
	self.Version = n.version
	self.SubPrints = make([]SubPrint, len(n.children))
	self.SubTree = n.treeValue != nil
	for index, child := range n.children {
		if child != nil {
			self.SubPrints[index] = SubPrint{
				Exists: true,
				Key:    append(append([]Nibble{}, self.Key...), child.segment...),
				Sum:    child.hash,
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

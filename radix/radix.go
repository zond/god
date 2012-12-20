package radix

import (
	"bytes"
	"fmt"
	"unsafe"
)

var encodeChars = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f"}

type Nibble byte

type Timer interface {
	ContinuousTime() int64
}

func toBytes(n []Nibble) []byte {
	return *((*[]byte)(unsafe.Pointer(&n)))
}

const (
	parts    = 2
	mirrored = "mirrored"
	yes      = "yes"
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
	Timestamp int64
	SubTree   bool
	SubPrints []SubPrint
	ByteHash  []byte
	TreeHash  []byte
}

func (self *Print) coveredBy(other *Print) bool {
	if self == nil {
		return other == nil
	}
	return other != nil && (other.Timestamp > self.Timestamp || bytes.Compare(self.ByteHash, other.ByteHash) == 0)
}
func (self *Print) push(n *node) {
	self.Key = append(self.Key, n.segment...)
}
func (self *Print) set(n *node) {
	self.Exists = true
	self.ByteHash = n.byteHash
	self.TreeHash = n.treeValue.Hash()
	self.Empty = n.empty
	self.Timestamp = n.timestamp
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
func (self *Print) timestamp() int64 {
	if self == nil {
		return 0
	}
	return self.Timestamp
}

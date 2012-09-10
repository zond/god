
package murmur

import (
	"unsafe"
	"bytes"
)

/*
#include "murmur.h"
*/
import "C"

func init() {
	if unsafe.Sizeof(new(C.uint64_t)) != 8 {
		panic("C.word64 is not 8 bytes long!")
	}
}

const (
	BlockSize = 1
	Size = 16
	seed = 42
)

type Hash bytes.Buffer

func (self *Hash) Sum(p []byte) (result []byte) {
	(*bytes.Buffer)(self).Write(p)
	result = make([]byte, Size)
	buf := (*bytes.Buffer)(self).Bytes()
	C.MurmurHash3_x64_128(
		*(*unsafe.Pointer)(unsafe.Pointer(&buf)), 
		C.int(len(buf)), 
		C.uint32_t(seed), 
		*(*unsafe.Pointer)(unsafe.Pointer(&result)))
	self.Reset()
	return
}
func (self *Hash) Write(p []byte) (n int, err error) {
	return (*bytes.Buffer)(self).Write(p)
}
func (self *Hash) Reset() {
	(*bytes.Buffer)(self).Truncate(0)
}
func (self *Hash) Size() int {
	return 3 * 8
}
func (self *Hash) BlockSize() int {
	return 8
}

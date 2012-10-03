package murmur

import (
	"bytes"
	"fmt"
	"unsafe"
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
	Size      = 16
	seed      = 42
)

func HashString(s string) []byte {
	return NewBytes([]byte(s)).Get()
}

func HashInt(i int) []byte {
	return NewString(fmt.Sprint(i)).Get()
}

func HashInt64(i int64) []byte {
	return NewString(fmt.Sprint(i)).Get()
}

func HashBytes(b []byte) []byte {
	return NewBytes(b).Get()
}

type Hash bytes.Buffer

func New() *Hash {
	return new(Hash)
}
func NewString(s string) *Hash {
	return (*Hash)(bytes.NewBufferString(s))
}
func NewBytes(b []byte) *Hash {
	return (*Hash)(bytes.NewBuffer(b))
}
func (self *Hash) Get() (result []byte) {
	result = make([]byte, Size)
	self.Extrude(&result)
	return
}
func (self *Hash) Extrude(result *[]byte) {
	buf := (*bytes.Buffer)(self).Bytes()
	C.MurmurHash3_x64_128(
		*(*unsafe.Pointer)(unsafe.Pointer(&buf)),
		C.int(len(buf)),
		C.uint32_t(seed),
		*(*unsafe.Pointer)(unsafe.Pointer(result)))
	self.Reset()
}
func (self *Hash) Sum(p []byte) []byte {
	(*bytes.Buffer)(self).Write(p)
	return self.Get()
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

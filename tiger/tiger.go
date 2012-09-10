
package tree

import (
	"unsafe"
	"bytes"
)

/*
#include "tiger.h"
*/
import "C"

func init() {
	if unsafe.Sizeof(new(C.word64)) != 8 {
		panic("C.word64 is not 8 bytes long!")
	}
}

type Tiger bytes.Buffer

func (self *Tiger) Sum(p []byte) (result []byte) {
	var buf []byte
	if (*bytes.Buffer)(self).Len() == 0 && len(p) % 8 == 0 {
		buf = p
	} else {
		self.Write(p)
		rest := (*bytes.Buffer)(self).Len() % 8
		if rest != 0 {
			self.Write(make([]byte, 8 - rest))
		}
		buf = (*bytes.Buffer)(self).Bytes()
	}
	result = make([]byte, 3 * 8)
	C.tiger(*(**C.word64)(unsafe.Pointer(&buf)), C.word64(len(buf) / 8), *(**C.word64)(unsafe.Pointer(&result)))
	self.Reset()
	return
}
func (self *Tiger) Write(p []byte) (n int, err error) {
	return (*bytes.Buffer)(self).Write(p)
}
func (self *Tiger) Reset() {
	(*bytes.Buffer)(self).Truncate(0)
}
func (self *Tiger) Size() int {
	return 3 * 8
}
func (self *Tiger) BlockSize() int {
	return 8
}

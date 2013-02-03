package murmur

import (
  "bytes"
  "encoding/binary"
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

// HashString will return the hash for the provided string.
func HashString(s string) []byte {
  return NewBytes([]byte(s)).Get()
}

// HashInt will return the hash for the provided int.
func HashInt(i int) []byte {
  b := new(bytes.Buffer)
  if err := binary.Write(b, binary.BigEndian, i); err != nil {
    panic(err)
  }
  return NewBytes(b.Bytes()).Get()
}

// HashInt64 will return the hash for the provided int64.
func HashInt64(i int64) []byte {
  b := new(bytes.Buffer)
  if err := binary.Write(b, binary.BigEndian, i); err != nil {
    panic(err)
  }
  return NewBytes(b.Bytes()).Get()
}

// HashBytes will return the hash for the provided byte slice.
func HashBytes(b []byte) []byte {
  return NewBytes(b).Get()
}

// Hash is a thin thin wrapper around the reference implementation of http://code.google.com/p/smhasher/wiki/MurmurHash3 modified slightly to compile with a C compiler.
type Hash bytes.Buffer

func New() *Hash {
  return new(Hash)
}

// NewString will return a Hash that has already consumed the provided string.
func NewString(s string) *Hash {
  return (*Hash)(bytes.NewBufferString(s))
}

// NewBytes will return a Hash that has already consumed the provided byte slice.
func NewBytes(b []byte) *Hash {
  return (*Hash)(bytes.NewBuffer(b))
}

// Get will return the hash for all consumed data.
func (self *Hash) Get() (result []byte) {
  result = make([]byte, Size)
  self.Extrude(&result)
  return
}

// Extrude will fill the provided slice with the hash for all consumed data.
// Extrude is slightly less allocating than Get, if that kind of thing is of interest.
func (self *Hash) Extrude(result *[]byte) {
  buf := (*bytes.Buffer)(self).Bytes()
  C.MurmurHash3_x64_128(
    *(*unsafe.Pointer)(unsafe.Pointer(&buf)),
    C.int(len(buf)),
    C.uint32_t(seed),
    *(*unsafe.Pointer)(unsafe.Pointer(result)))
  self.Reset()
}

// Sum will consume the given bytes and return the hash for all consumed data.
func (self *Hash) Sum(p []byte) []byte {
  (*bytes.Buffer)(self).Write(p)
  return self.Get()
}

// MustWrite will consume the provided bytes.
func (self *Hash) MustWrite(p []byte) {
  if i, err := (*bytes.Buffer)(self).Write(p); i != len(p) || err != nil {
    panic(fmt.Errorf("When Writing %v to %v, got %v, %v", p, self, i, err))
  }
}

// Write will consume the provided bytes, or return an error.
func (self *Hash) Write(p []byte) (n int, err error) {
  return (*bytes.Buffer)(self).Write(p)
}

// Reset will forget all consumed data.
func (self *Hash) Reset() {
  (*bytes.Buffer)(self).Truncate(0)
}
func (self *Hash) Size() int {
  return 3 * 8
}
func (self *Hash) BlockSize() int {
  return 8
}

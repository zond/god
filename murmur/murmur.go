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

func hash32(key unsafe.Pointer, length C.int, seed C.uint32_t, out unsafe.Pointer) {
	C.MurmurHash3_x86_128(key, length, seed, out)
}

func hash64(key unsafe.Pointer, length C.int, seed C.uint32_t, out unsafe.Pointer) {
	C.MurmurHash3_x64_128(key, length, seed, out)
}

var hashfunc func(key unsafe.Pointer, lenght C.int, seed C.uint32_t, out unsafe.Pointer)

func init() {
	testtype := new(C.uint64_t)
	if unsafe.Sizeof(testtype) == 8 {
		hashfunc = hash64
	} else if unsafe.Sizeof(testtype) == 4 {
		hashfunc = hash32
	} else {
		panic("C.word64 is not 4 or 8 bytes long!")
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

func (self *Hash) HashBytes(b []byte) []byte {
	self.Reset()
	self.Write(b)
	return self.Get()
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
	hashfunc(
		*(*unsafe.Pointer)(unsafe.Pointer(&buf)),
		C.int(len(buf)),
		C.uint32_t(seed),
		*(*unsafe.Pointer)(unsafe.Pointer(result)))
	self.Reset()
}

// Sum appends the current hash to b and returns the resulting slice.
// It does not change the underlying hash state.
func (self *Hash) Sum(p []byte) []byte {
	return append(p, self.Get()...)
}

func (self *Hash) MustWriteInt64(i int64) {
	b := new(bytes.Buffer)
	if err := binary.Write(b, binary.BigEndian, i); err != nil {
		panic(err)
	}
	self.MustWrite(b.Bytes())
}

// MustWrite adds more data to the running hash.
func (self *Hash) MustWrite(p []byte) {
	if i, err := (*bytes.Buffer)(self).Write(p); i != len(p) || err != nil {
		panic(fmt.Errorf("When Writing %v to %v, got %v, %v", p, self, i, err))
	}
}

// Write adds more data to the running hash.
// It never returns an error.
func (self *Hash) Write(p []byte) (n int, err error) {
	return (*bytes.Buffer)(self).Write(p)
}

// Reset resets the hash to one with zero bytes written.
func (self *Hash) Reset() {
	(*bytes.Buffer)(self).Truncate(0)
}

// Size returns the number of bytes Sum will return.
func (self *Hash) Size() int {
	return 3 * 8
}

// BlockSize returns the hash's underlying block size.
// The Write method must be able to accept any amount
// of data, but it may operate more efficiently if all writes
// are a multiple of the block size.
func (self *Hash) BlockSize() int {
	return 8
}

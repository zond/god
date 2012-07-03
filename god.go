
package god

import (
	"github.com/zond/gotomic"
	"bytes"
	"os"
	"hash/crc32"
)

const exponent = 9

const (
	hashvalue = iota
)

type key string
func (self key) HashCode() uint32 {
	return crc32.ChecksumIEEE([]byte(self))
}
func (self key) Equals(t gotomic.Thing) bool {
	if b, ok := t.(key); ok {
		return bytes.Equal([]byte(self), []byte(b))
	}
	return false
}

type God struct {
	hashes []*gotomic.Hash
	logfile *os.File
}
func NewGod(path string) (rval *God, err error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	rval = &God{make([]*gotomic.Hash, 1 << exponent), file}
	for i := 0; i < len(rval.hashes); i++ {
		rval.hashes[i] = gotomic.NewHash()
	}
	return rval, nil
}
func (self *God) put(k string, v []byte) []byte {
	key := key(k)
	shard, hashCode := self.shard(key)
	t := shard.PutHC(hashCode, key, v)
	if t == nil {
		return nil
	}
	return t.([]byte)
}
func (self *God) Put(k string, v []byte) []byte {
	if _, err := self.logfile.Write(v); err != nil {
		panic(err)
	}
	return self.put(k, v)
}
func (self *God) mark(b []byte, t byte) []byte {
	rval := make([]byte, len(b) + 1)
	rval[0] = t
	copy(rval[1:], b)
	return rval
}
func (self *God) shard(h gotomic.Hashable) (hash *gotomic.Hash, hashCode uint32)  {
	hashCode = h.HashCode()
	hash = self.hashes[hashCode & ((1 << exponent) - 1)]
	return
}
func (self *God) Get(k string) (b []byte, ok bool) {
	key := key(k)
	hash, hc := self.shard(key)
	if t, ok := hash.GetHC(hc, key); ok {
		return t.([]byte), true
	}
	return nil, false
}

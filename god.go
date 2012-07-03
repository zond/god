
package god

import (
	"github.com/zond/gotomic"
	"github.com/zond/cabinet"
	"bytes"
	"hash/crc32"
	"fmt"
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
	cabinet *cabinet.KCDB
}
func NewGod(path string) (rval *God, err error) {
	rval = &God{make([]*gotomic.Hash, 1 << exponent), cabinet.New()}
	if err = rval.cabinet.Open(fmt.Sprint(path, ".kch"), cabinet.KCOWRITER | cabinet.KCOCREATE); err != nil {
		panic(err)
	}
	rval.reload()
	return rval, nil
}
func (self *God) reload() {
	for i := 0; i < len(self.hashes); i++ {
		self.hashes[i] = gotomic.NewHash()
	}
	cursor := self.cabinet.Cursor()
	k, v, err := cursor.Get(true)
	for err == nil {
		self.reinsert(k, v)
		k, v, err = cursor.Get(true)
	}
}
func (self *God) reinsert(k, v []byte) {
	switch v[0] {
	case hashvalue:
		self.put(string(k), v[1:])
	default:
		panic(fmt.Errorf("Unknown data type %v", v[0]))
	}
}
func (self *God) persist(k string, v []byte) {
	if err := self.cabinet.Set([]byte(k), v); err != nil {
		panic(err)
	}
	if err := self.cabinet.Sync(false); err != nil {
		panic(err)
	}
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
	self.persist(k, self.mark(v, hashvalue))
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

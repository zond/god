
package radix

import (
	"bytes"
)

type HashTree interface {
	Hash() []byte
	Finger(key []byte) *Print
	Put(key []byte, value Hasher) (old Hasher, existed bool)
	Get(key []byte) (value Hasher, existed bool)
	Del(key []byte) (old Hasher, existed bool)
}

type Sync struct {
	source HashTree
	destination HashTree
	sourcePrint *Print
	destinationPrint *Print
}
func (self *Sync) Tick() bool {
	if bytes.Compare(self.source.Hash(), self.destination.Hash()) == 0 {
		self.sourcePrint = nil
		self.destinationPrint = nil
		return false
	}
	self.sourcePrint, self.destinationPrint = self.source.Finger(nil), self.destination.Finger(nil)
	return self.tickPrint()
}
func (self *Sync) tickPrint() bool {
	if bytes.Compare(self.sourcePrint.ValueHash, self.destinationPrint.ValueHash) != 0 {
		if value, existed := self.source.Get(self.sourcePrint.Key); existed {
			self.destination.Put(self.sourcePrint.Key, value)
			return true
		}
	}
	return false
}

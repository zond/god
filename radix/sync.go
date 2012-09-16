
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
	if self.sourcePrint.ValueHash == nil {
		// If there isn't supposed to be a value here
		if self.destinationPrint != nil && self.destinationPrint.ValueHash != nil {
			// But the destination has one
			self.destination.Del(self.sourcePrint.Key)
			return true
		}
	} else {
		// If there is supposed to be a value here
		if self.destinationPrint == nil || bytes.Compare(self.sourcePrint.ValueHash, self.destinationPrint.ValueHash) != 0 {
			// But the destination has none, or the wrong one
			if value, existed := self.source.Get(self.sourcePrint.Key); existed {
				self.destination.Put(self.sourcePrint.Key, value)
				return true
			}
		}
	}
	for i := 0; i < 1 << parts; i++ {
		if bytes.Compare(self.sourcePrint.SubPrints[i].Sum, self.destinationPrint.SubPrints[i].Sum) != 0 {	
			// FIX THIS!
				// If the destination does not exist, we need to do something about that. If the source does not but
				// the destination does, likewise
			self.sourcePrint = self.source.Finger(self.sourcePrint.SubPrints[i].Key)
			self.destinationPrint = self.destination.Finger(self.destinationPrint.SubPrints[i].Key)
			return self.tickPrint()
		}
	}
	self.sourcePrint = nil
	self.destinationPrint = nil
	return false
}

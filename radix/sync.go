
package radix

import (
	"bytes"
	"fmt"
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
}
func NewSync(source, destination HashTree) *Sync {
	return &Sync{
		source: source,
		destination: destination,
	}
}
func (self *Sync) Tick() bool {
	if bytes.Compare(self.source.Hash(), self.destination.Hash()) == 0 {
		return false
	}
	sourcePrint, destinationPrint := self.source.Finger(nil), self.destination.Finger(nil)
	fmt.Println("*********** prints: ", sourcePrint, destinationPrint)
	return self.tickPrint(sourcePrint, destinationPrint)
}
func (self *Sync) Run() {
	for {
		if !self.Tick() {
			return
		}
	}
}
func (self *Sync) cpy(key []byte) bool {
	if value, existed := self.source.Get(key); existed {
		old, _ := self.destination.Put(key, value)
		fmt.Println("copied", string(key), "=>", old, "to", value)
		return true
	}
	return false
}
func (self *Sync) del(key []byte) bool {
	_, existed := self.destination.Del(key)
	return existed
}
func (self *Sync) tickPrint(sourcePrint, destinationPrint *Print) bool {
	if sourcePrint.ValueHash == nil {
		// If there isn't supposed to be a value here
		if destinationPrint != nil && destinationPrint.ValueHash != nil {
			// But the destination has one
			if self.del(stitch(sourcePrint.Key)) {
				return true
			}
		}
	} else {
		// If there is supposed to be a value here
		if destinationPrint == nil || bytes.Compare(sourcePrint.ValueHash, destinationPrint.ValueHash) != 0 {
			// But the destination has none, or the wrong one
			fmt.Println("copying since dest and source are unequal", sourcePrint, destinationPrint)
			if self.cpy(stitch(sourcePrint.Key)) {
				return true
			}
		}
	}
	for i := 0; i < 1 << parts; i++ {
		if bytes.Compare(sourcePrint.SubPrints[i].Sum, destinationPrint.SubPrints[i].Sum) != 0 {	
			sourcePrint = self.source.Finger(sourcePrint.SubPrints[i].Key)
			destinationPrint = self.destination.Finger(destinationPrint.SubPrints[i].Key)
			fmt.Println("########### new prints: ", sourcePrint, destinationPrint)
			return self.tickPrint(sourcePrint, destinationPrint)
		}
	}
	return false
}

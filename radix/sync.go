
package radix

import (
	"bytes"
	"fmt"
)

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
	fmt.Println("Tick!")
	if bytes.Compare(self.source.Hash(), self.destination.Hash()) == 0 {
		return false
	}
	return self.tickPrint(self.source.Finger([]byte{}), self.destination.Finger([]byte{}))
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
func (self *Sync) tickPrint(sourcePrint, destinationPrint *Print) (changed bool) {
	fmt.Println("comparing", sourcePrint, "and", destinationPrint)
	if sourcePrint == nil || sourcePrint.ValueHash == nil {
		// If there isn't supposed to be a value here
		if destinationPrint != nil && destinationPrint.ValueHash != nil {
			// But the destination has one
			if _, existed := self.destination.Del(stitch(sourcePrint.Key)); existed {
				changed = true
			}
		}
	} else {
		// If there is supposed to be a value here
		if destinationPrint == nil || bytes.Compare(sourcePrint.ValueHash, destinationPrint.ValueHash) != 0 {
			// But the destination has none, or the wrong one
			key := stitch(sourcePrint.Key)
			if value, existed := self.source.Get(key); existed {
				fmt.Println("inserted", sourcePrint.Key)
				self.destination.Put(key, value)
				changed = true
			}
		}
	}
	for i := 0; i < 1 << parts; i++ {
		if bytes.Compare(sourcePrint.SubPrints[i].Sum, destinationPrint.SubPrints[i].Sum) != 0 {	
			fmt.Println("recursing into", sourcePrint.SubPrints[i].Key, destinationPrint.SubPrints[i].Key)
			sourcePrint = self.source.Finger(sourcePrint.SubPrints[i].Key)
			destinationPrint = self.destination.Finger(destinationPrint.SubPrints[i].Key)
			if self.tickPrint(sourcePrint, destinationPrint) {
				changed = true
				break
			}
		}
	}
	return changed
}

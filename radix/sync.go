
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
func (self *Sync) Run() bool {
	if bytes.Compare(self.source.Hash(), self.destination.Hash()) == 0 {
		return false
	}
	self.synchronize(self.source.Finger(nil), self.destination.Finger(nil))
	return true
}
func (self *Sync) synchronize(sourcePrint, destinationPrint *Print) {
	fmt.Println("synchronizing", sourcePrint, "*** AND ***",destinationPrint)
	if sourcePrint == nil || sourcePrint.ValueHash == nil {
		// If there isn't supposed to be a value here
		if destinationPrint != nil && destinationPrint.ValueHash != nil {
			// But the destination has one
			self.destination.Del(stitch(sourcePrint.Key))
		}
	} else {
		// If there is supposed to be a value here
		if destinationPrint == nil || bytes.Compare(sourcePrint.ValueHash, destinationPrint.ValueHash) != 0 {
			// But the destination has none, or the wrong one
			key := stitch(sourcePrint.Key)
			if value, existed := self.source.Get(key); existed {
				fmt.Println("*** inserting", key)
				self.destination.Put(key, value)
			}
		}
	}
	for i := 0; i < 1 << parts; i++ {
		if bytes.Compare(sourcePrint.SubPrints[i].Sum, destinationPrint.SubPrints[i].Sum) != 0 {	
			self.synchronize(
				self.source.Finger(sourcePrint.SubPrints[i].Key), 
				self.destination.Finger(destinationPrint.SubPrints[i].Key),
				)
		}
	}
}

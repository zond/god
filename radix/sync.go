package radix

import (
	"bytes"
)

type Sync struct {
	source      HashTree
	destination HashTree
	from        []byte
	to          []byte
}

func NewSync(source, destination HashTree) *Sync {
	return &Sync{
		source:      source,
		destination: destination,
	}
}

/*
 Inclusive
*/
func (self *Sync) From(from []byte) *Sync {
	self.from = from
	return self
}

/*
 Exclusive
*/
func (self *Sync) To(to []byte) *Sync {
	self.to = to
	return self
}
func (self *Sync) Run() bool {
	if bytes.Compare(self.source.Hash(), self.destination.Hash()) == 0 {
		return false
	}
	self.synchronize(self.source.Finger(nil), self.destination.Finger(nil))
	return true
}
func (self *Sync) synchronize(sourcePrint, destinationPrint *Print) {
	if sourcePrint != nil {
		if self.from == nil || bytes.Compare(self.from, sourcePrint.Key) > -1 {
			if self.to == nil || bytes.Compare(self.to, sourcePrint.Key) > 0 {
				if destinationPrint == nil || bytes.Compare(sourcePrint.ValueHash, destinationPrint.ValueHash) != 0 {
					key := stitch(sourcePrint.Key)
					if value, existed := self.source.Get(key); existed {
						self.destination.Put(key, value)
					}
				}
			}
		}
	}
	for index, subPrint := range sourcePrint.SubPrints {
		if subPrint != nil {
			if destinationPrint == nil ||
				destinationPrint.SubPrints[index] == nil ||
				bytes.Compare(subPrint.Sum, destinationPrint.SubPrints[index].Sum) != 0 {
				self.synchronize(
					self.source.Finger(subPrint.Key),
					self.destination.Finger(subPrint.Key),
				)
			}
		}
	}
}

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
	self.from = rip(from)
	return self
}

/*
Exclusive
*/
func (self *Sync) To(to []byte) *Sync {
	self.to = rip(to)
	return self
}
func (self *Sync) Run() bool {
	if bytes.Compare(self.source.Hash(), self.destination.Hash()) == 0 {
		return false
	}
	self.synchronize(self.source.Finger(nil), self.destination.Finger(nil))
	return true
}
func (self *Sync) withinLimits(key []byte) bool {
	if self.from == nil || bytes.Compare(key, self.from) > -1 {
		if self.withinRightLimit(key) {
			return true
		}
	}
	return false
}
func (self *Sync) withinRightLimit(key []byte) bool {
	if self.to == nil || bytes.Compare(key, self.to) < 0 {
		return true
	}
	return false
}
func (self *Sync) synchronize(sourcePrint, destinationPrint *Print) {
	if sourcePrint != nil && self.withinLimits(sourcePrint.Key) {
		// If there is a node at source	and it is within our limits	
		if !sourcePrint.match(destinationPrint) {
			// If the key at destination is missing or wrong				
			key := stitch(sourcePrint.Key)
			if value, existed := self.source.Get(key); existed {
				self.destination.Put(key, value)
			}
		}
	}
	for index, subPrint := range sourcePrint.SubPrints {
		if subPrint != nil && self.withinRightLimit(subPrint.Key) {
			if destinationPrint == nil || !subPrint.match(destinationPrint.SubPrints[index]) {
				self.synchronize(
					self.source.Finger(subPrint.Key),
					self.destination.Finger(subPrint.Key),
				)
			}
		}
	}
}

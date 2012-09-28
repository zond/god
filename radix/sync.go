package radix

import (
	"bytes"
)

type HashTree interface {
	Hash() []byte
	Finger(key []byte) *Print
	PutVersion(key []byte, value Hasher, version uint32) (old Hasher, oldVersion uint32, existed bool)
	GetVersion(key []byte) (value Hasher, version uint32, existed bool)
	Del(key []byte) (old Hasher, existed bool)
}

type Sync struct {
	source      HashTree
	destination HashTree
	from        []byte
	to          []byte
	destructive bool
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
func (self *Sync) Destroy() *Sync {
	self.destructive = true
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
func (self *Sync) stitch(ripped []byte, stitched *[]byte) {
	if *stitched == nil {
		*stitched = stitch(ripped)
	}
}
func (self *Sync) synchronize(sourcePrint, destinationPrint *Print) {
	if sourcePrint != nil && sourcePrint.ValueHash != nil && self.withinLimits(sourcePrint.Key) {
		// If there is a node at source	and it is within our limits	
		var key []byte
		if !sourcePrint.coveredBy(destinationPrint) {
			// If the key at destination is missing or wrong				
			self.stitch(sourcePrint.Key, &key)
			if value, version, existed := self.source.GetVersion(key); existed {
				self.destination.PutVersion(key, value, version)
			}
		}
		if self.destructive && sourcePrint.ValueHash != nil {
			self.stitch(sourcePrint.Key, &key)
			self.source.Del(key)
		}
	}
	for index, subPrint := range sourcePrint.SubPrints {
		if subPrint != nil && self.withinRightLimit(subPrint.Key) {
			if destinationPrint == nil || !subPrint.equals(destinationPrint.SubPrints[index]) {
				self.synchronize(
					self.source.Finger(subPrint.Key),
					self.destination.Finger(subPrint.Key),
				)
			}
		}
	}
}

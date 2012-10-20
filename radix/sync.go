package radix

import (
	"bytes"
)

const (
	subTreeError = "Illegal, only one level of sub trees supported"
)

type HashTree interface {
	Hash() []byte

	Finger(key []Nibble) *Print
	GetVersion(key []Nibble) (value Hasher, version int64, existed bool)
	PutVersion(key []Nibble, value Hasher, expected, version int64)
	DelVersion(key []Nibble, expected int64)

	SubFinger(key, subKey []Nibble, expected int64) (result *Print)
	SubGetVersion(key, subKey []Nibble, expected int64) (value Hasher, version int64, existed bool)
	SubPutVersion(key, subKey []Nibble, value Hasher, expected, subExpected, subVersion int64)
	SubDelVersion(key, subKey []Nibble, expected, subExpected int64)
}

type Sync struct {
	source      HashTree
	destination HashTree
	from        []Nibble
	to          []Nibble
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
func (self *Sync) withinLimits(key []Nibble) bool {
	if self.from == nil || bytes.Compare(toBytes(key), toBytes(self.from)) > -1 {
		if self.withinRightLimit(key) {
			return true
		}
	}
	return false
}
func (self *Sync) withinRightLimit(key []Nibble) bool {
	if self.to == nil || bytes.Compare(toBytes(key), toBytes(self.to)) < 0 {
		return true
	}
	return false
}
func (self *Sync) synchronize(sourcePrint, destinationPrint *Print) {
	if sourcePrint != nil {
		if sourcePrint.ValueHash != nil && self.withinLimits(sourcePrint.Key) {
			// If there is a node at source	and it is within our limits	
			if !sourcePrint.coveredBy(destinationPrint) {
				// If the key at destination is missing or wrong				
				if sourcePrint.SubTree {
					NewSync(&subTreeWrapper{self.source, sourcePrint.Key, sourcePrint.version()}, &subTreeWrapper{self.destination, sourcePrint.Key, destinationPrint.version()}).Run()
				} else {
					if value, version, existed := self.source.GetVersion(sourcePrint.Key); existed && version == sourcePrint.version() {
						self.destination.PutVersion(sourcePrint.Key, value, destinationPrint.version(), sourcePrint.version())
					}
				}
			}
			if self.destructive && sourcePrint.ValueHash != nil {
				self.source.DelVersion(sourcePrint.Key, sourcePrint.version())
			}
		}
		for index, subPrint := range sourcePrint.SubPrints {
			if subPrint.Key != nil && self.withinRightLimit(subPrint.Key) {
				if destinationPrint == nil || !subPrint.equals(destinationPrint.SubPrints[index]) {
					self.synchronize(
						self.source.Finger(subPrint.Key),
						self.destination.Finger(subPrint.Key),
					)
				}
			}
		}
	}
}

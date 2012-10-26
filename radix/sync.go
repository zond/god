package radix

import (
	"../common"
	"bytes"
)

const (
	subTreeError = "Illegal, only one level of sub trees supported"
)

type HashTree interface {
	Hash() []byte

	Finger(key []Nibble) *Print
	GetVersion(key []Nibble) (value Hasher, version int64, existed bool)
	PutVersion(key []Nibble, value Hasher, expected, version int64) bool
	DelVersion(key []Nibble, expected int64) bool

	SubFinger(key, subKey []Nibble, expected int64) (result *Print)
	SubGetVersion(key, subKey []Nibble, expected int64) (value Hasher, version int64, existed bool)
	SubPutVersion(key, subKey []Nibble, value Hasher, expected, subExpected, subVersion int64) bool
	SubDelVersion(key, subKey []Nibble, expected, subExpected int64) bool
}

type Sync struct {
	source      HashTree
	destination HashTree
	from        []Nibble
	to          []Nibble
	destructive bool
	putCount    int
	delCount    int
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
func (self *Sync) PutCount() int {
	return self.putCount
}
func (self *Sync) DelCount() int {
	return self.delCount
}
func (self *Sync) Run() *Sync {
	if self.destructive || bytes.Compare(self.source.Hash(), self.destination.Hash()) != 0 {
		self.synchronize(self.source.Finger(nil), self.destination.Finger(nil))
	}
	return self
}
func (self *Sync) potentiallyWithinLimits(key []Nibble) bool {
	if self.from == nil || self.to == nil {
		return true
	}
	return common.BetweenII(toBytes(key), toBytes(self.from)[:len(key)], toBytes(self.to)[:len(key)])
}
func (self *Sync) withinLimits(key []Nibble) bool {
	if self.from == nil || self.to == nil {
		return true
	}
	return common.BetweenIE(toBytes(key), toBytes(self.from), toBytes(self.to))
}
func (self *Sync) synchronize(sourcePrint, destinationPrint *Print) {
	if sourcePrint.Exists {
		if sourcePrint.ValueHash != nil && self.withinLimits(sourcePrint.Key) {
			// If there is a node at source	and it is within our limits	
			var subPut int
			if !sourcePrint.coveredBy(destinationPrint) {
				// If the key at destination is missing or wrong				
				if sourcePrint.SubTree {
					subPut += NewSync(&subTreeWrapper{
						self.source,
						sourcePrint.Key,
						sourcePrint.version(),
					}, &subTreeWrapper{
						self.destination,
						sourcePrint.Key,
						destinationPrint.version(),
					}).Run().PutCount()
					self.putCount += subPut
				} else {
					if value, version, existed := self.source.GetVersion(sourcePrint.Key); existed && version == sourcePrint.version() {
						if self.destination.PutVersion(sourcePrint.Key, value, destinationPrint.version(), sourcePrint.version()) {
							self.putCount++
						}
					}
				}
			}
			if self.destructive && sourcePrint.ValueHash != nil {
				if self.source.DelVersion(sourcePrint.Key, sourcePrint.version()) {
					if sourcePrint.SubTree {
						self.delCount += subPut
					} else {
						self.delCount++
					}
				}
			}
		}
		for index, subPrint := range sourcePrint.SubPrints {
			if subPrint.Exists && self.potentiallyWithinLimits(subPrint.Key) {
				if self.destructive || (!destinationPrint.Exists || !subPrint.equals(destinationPrint.SubPrints[index])) {
					self.synchronize(
						self.source.Finger(subPrint.Key),
						self.destination.Finger(subPrint.Key),
					)
				}
			}
		}
	}
}

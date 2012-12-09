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
	GetTimestamp(key []Nibble) (byteValue []byte, timestamp int64, existed bool)
	PutTimestamp(key []Nibble, byteValue []byte, expected, timestamp int64) bool
	DelTimestamp(key []Nibble, expected int64) bool

	SubFinger(key, subKey []Nibble, expected int64) (result *Print)
	SubGetTimestamp(key, subKey []Nibble, expected int64) (byteValue []byte, timestamp int64, existed bool)
	SubPutTimestamp(key, subKey []Nibble, byteValue []byte, expected, subExpected, subTimestamp int64) bool
	SubDelTimestamp(key, subKey []Nibble, expected, subExpected int64) bool
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
	self.from = Rip(from)
	return self
}

/*
Exclusive
*/
func (self *Sync) To(to []byte) *Sync {
	self.to = Rip(to)
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
	// If we have from and to, and they are equal, that means this sync is over an empty set... just ignore it
	if self.from != nil && self.to != nil && nComp(self.from, self.to) == 0 {
		return self
	}
	if self.destructive || bytes.Compare(self.source.Hash(), self.destination.Hash()) != 0 {
		self.synchronize(self.source.Finger(nil), self.destination.Finger(nil))
	}
	return self
}
func (self *Sync) potentiallyWithinLimits(key []Nibble) bool {
	if self.from == nil || self.to == nil {
		return true
	}
	cmpKey := toBytes(key)
	cmpFrom := toBytes(self.from)
	cmpTo := toBytes(self.to)
	m := len(cmpKey)
	if m > len(cmpFrom) {
		m = len(cmpFrom)
	}
	if m > len(cmpTo) {
		m = len(cmpTo)
	}
	return common.BetweenII(cmpKey[:m], cmpFrom[:m], cmpTo[:m])
}
func (self *Sync) withinLimits(key []Nibble) bool {
	if self.from == nil || self.to == nil {
		return true
	}
	return common.BetweenIE(toBytes(key), toBytes(self.from), toBytes(self.to))
}
func (self *Sync) synchronize(sourcePrint, destinationPrint *Print) {
	if sourcePrint.Exists {
		if !sourcePrint.Empty && self.withinLimits(sourcePrint.Key) {
			// If there is a node at source	and it is within our limits	
			var subPut int
			if !sourcePrint.coveredBy(destinationPrint) {
				// If the key at destination is missing or wrong				
				if sourcePrint.SubTree {
					subSync := NewSync(&subTreeWrapper{
						self.source,
						sourcePrint.Key,
						sourcePrint.timestamp(),
					}, &subTreeWrapper{
						self.destination,
						sourcePrint.Key,
						destinationPrint.timestamp(),
					})
					if self.destructive {
						subSync.Destroy()
					}
					subPut += subSync.Run().PutCount()
					self.putCount += subPut
				}
				if value, timestamp, existed := self.source.GetTimestamp(sourcePrint.Key); existed && timestamp == sourcePrint.timestamp() {
					if self.destination.PutTimestamp(sourcePrint.Key, value, destinationPrint.timestamp(), sourcePrint.timestamp()) {
						self.putCount++
					}
				}
			}
			if self.destructive && !sourcePrint.Empty {
				if self.source.DelTimestamp(sourcePrint.Key, sourcePrint.timestamp()) {
					if sourcePrint.SubTree {
						self.delCount += subPut
					}
					self.delCount++
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

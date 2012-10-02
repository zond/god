package radix

import (
	"bytes"
)

const (
	subTreeError = "Illegal, only one level of sub trees supported"
)

type HashTree interface {
	Hash() []byte

	Finger(key []nibble) *Print
	GetVersion(key []nibble) (value Hasher, version uint32, existed bool)
	PutVersion(key []nibble, value Hasher, expected, version uint32)
	DelVersion(key []nibble, expected uint32)

	SubFinger(key, subKey []nibble, expected uint32) (result *Print)
	SubGetVersion(key, subKey []nibble, expected uint32) (value Hasher, version uint32, existed bool)
	SubPutVersion(key, subKey []nibble, value Hasher, expected, subExpected, subVersion uint32)
	SubDelVersion(key, subKey []nibble, expected, subExpected uint32)
}

type subTreeWrapper struct {
	parentTree HashTree
	key        []nibble
	version    uint32
}

func (self *subTreeWrapper) Hash() (hash []byte) {
	if p := self.parentTree.Finger(self.key); p != nil {
		hash = p.ValueHash
	}
	return
}
func (self *subTreeWrapper) Finger(subKey []nibble) *Print {
	return self.parentTree.SubFinger(self.key, subKey, self.version)
}
func (self *subTreeWrapper) GetVersion(subKey []nibble) (value Hasher, version uint32, existed bool) {
	return self.parentTree.SubGetVersion(self.key, subKey, self.version)
}
func (self *subTreeWrapper) PutVersion(subKey []nibble, value Hasher, expected, version uint32) {
	self.parentTree.SubPutVersion(self.key, subKey, value, self.version, expected, version)
}
func (self *subTreeWrapper) DelVersion(subKey []nibble, expected uint32) {
	self.parentTree.SubDelVersion(self.key, subKey, self.version, expected)
}
func (self *subTreeWrapper) SubFinger(key, subKey []nibble, expected uint32) (result *Print) {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubGetVersion(key, subKey []nibble, expected uint32) (value Hasher, version uint32, existed bool) {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubPutVersion(key, subKey []nibble, value Hasher, expected, subExpected, subVersion uint32) {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubDelVersion(key, subKey []nibble, expected, subExpected uint32) {
	panic(subTreeError)
}

type Sync struct {
	source      HashTree
	destination HashTree
	from        []nibble
	to          []nibble
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
func (self *Sync) withinLimits(key []nibble) bool {
	if self.from == nil || bytes.Compare(toBytes(key), toBytes(self.from)) > -1 {
		if self.withinRightLimit(key) {
			return true
		}
	}
	return false
}
func (self *Sync) withinRightLimit(key []nibble) bool {
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
}

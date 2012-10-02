package radix

import (
	"bytes"
)

const (
	subTreeError = "Illegal, only one level of sub trees supported"
)

type HashTree interface {
	Hash() []byte
	Finger(key []byte) *Print
	PutVersion(key []byte, value Hasher, version uint32)
	GetVersion(key []byte, version uint32) (value Hasher, existed bool)
	DelVersion(key []byte, version uint32)
	SubFinger(key, subKey []byte, version uint32) (result *Print)
	SubPutVersion(key, subKey []byte, value Hasher, version, subVersion uint32)
	SubGetVersion(key, subKey []byte, version, subVersion uint32) (value Hasher, existed bool)
	SubDelVersion(key, subKey []byte, version, subVersion uint32)
}

type subTreeWrapper struct {
	parentTree HashTree
	key        []byte
	version    uint32
}

func (self *subTreeWrapper) Hash() (hash []byte) {
	if subPrint := self.parentTree.Finger(key); subPrint != nil {
		hash = subPrint.ValueHash
	}
	return
}
func (self *subTreeWrapper) Finger(subKey []byte) *Print {
	return self.parentTree.SubFinger(self.key, subKey, self.version)
}
func (self *subTreeWrapper) PutVersion(subKey []byte, value Hasher, version uint32) {
	self.parentTree.SubPutVersion(self.key, subKey, value, self.version, version)
}
func (self *subTreeWrapper) GetVersion(subKey []byte, version uint32) (value Hasher, existed bool) {
	return self.parentTree.SubGetVersion(self.key, subKey, self.version, version)
}
func (self *subTreeWrapper) DelVersion(subKey []byte, version uint32) {
	self.parentTree.SubDel(self.key, subKey, self.version, version)
}
func (self *subTreeWrapper) SubFinger(key, subKey []byte, version uint32) (result *Print) {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubPutVersion(key, subKey []byte, value Hasher, version, subVersion uint32) {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubGetVersion(key, subKey []byte, version, subVersion uint32) (value Hasher, existed bool) {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubDelVersion(key, subKey []byte, version, subVersion) {
	panic(subTreeError)
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
			if sourcePrint.SubTree {
				NewSync(&subTreeWrapper{self.source, key, sourcePrint.Version}, &subTreeWrapper{self.destination, key, sourcePrint.Version}).Run()
			} else {
				if value, existed := self.source.GetVersion(key, sourcePrint.Version); existed {
					self.destination.PutVersion(key, value, version)
				}
			}
		}
		if self.destructive && sourcePrint.ValueHash != nil {
			self.stitch(sourcePrint.Key, &key)
			self.source.Del(key, sourcePrint.Version)
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

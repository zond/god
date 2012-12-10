package radix

type subTreeWrapper struct {
	parentTree HashTree
	key        []Nibble
	version    int64
}

func (self *subTreeWrapper) Hash() (hash []byte) {
	if p := self.parentTree.Finger(self.key); p != nil {
		hash = p.TreeHash
	}
	return
}
func (self *subTreeWrapper) Finger(subKey []Nibble) *Print {
	return self.parentTree.SubFinger(self.key, subKey, self.version)
}
func (self *subTreeWrapper) GetTimestamp(subKey []Nibble) (byteValue []byte, version int64, present bool) {
	return self.parentTree.SubGetTimestamp(self.key, subKey, self.version)
}
func (self *subTreeWrapper) PutTimestamp(subKey []Nibble, byteValue []byte, present bool, expected, version int64) bool {
	return self.parentTree.SubPutTimestamp(self.key, subKey, byteValue, present, self.version, expected, version)
}
func (self *subTreeWrapper) DelTimestamp(subKey []Nibble, expected int64) bool {
	return self.parentTree.SubDelTimestamp(self.key, subKey, self.version, expected)
}
func (self *subTreeWrapper) SubFinger(key, subKey []Nibble, expected int64) (result *Print) {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubGetTimestamp(key, subKey []Nibble, expected int64) (byteValue []byte, version int64, present bool) {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubPutTimestamp(key, subKey []Nibble, byteValue []byte, present bool, expected, subExpected, subTimestamp int64) bool {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubDelTimestamp(key, subKey []Nibble, expected, subExpected int64) bool {
	panic(subTreeError)
}

package radix

type subTreeWrapper struct {
	parentTree HashTree
	key        []Nibble
	version    int64
}

func (self *subTreeWrapper) Hash() (hash []byte) {
	if p := self.parentTree.Finger(self.key); p != nil {
		hash = p.ValueHash
	}
	return
}
func (self *subTreeWrapper) Finger(subKey []Nibble) *Print {
	return self.parentTree.SubFinger(self.key, subKey, self.version)
}
func (self *subTreeWrapper) GetVersion(subKey []Nibble) (value Hasher, version int64, existed bool) {
	return self.parentTree.SubGetVersion(self.key, subKey, self.version)
}
func (self *subTreeWrapper) PutVersion(subKey []Nibble, value Hasher, expected, version int64) {
	self.parentTree.SubPutVersion(self.key, subKey, value, self.version, expected, version)
}
func (self *subTreeWrapper) DelVersion(subKey []Nibble, expected int64) {
	self.parentTree.SubDelVersion(self.key, subKey, self.version, expected)
}
func (self *subTreeWrapper) SubFinger(key, subKey []Nibble, expected int64) (result *Print) {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubGetVersion(key, subKey []Nibble, expected int64) (value Hasher, version int64, existed bool) {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubPutVersion(key, subKey []Nibble, value Hasher, expected, subExpected, subVersion int64) {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubDelVersion(key, subKey []Nibble, expected, subExpected int64) {
	panic(subTreeError)
}

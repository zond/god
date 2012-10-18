package radix

type subTreeWrapper struct {
	parentTree HashTree
	key        []nibble
	version    int64
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
func (self *subTreeWrapper) GetVersion(subKey []nibble) (value Hasher, version int64, existed bool) {
	return self.parentTree.SubGetVersion(self.key, subKey, self.version)
}
func (self *subTreeWrapper) PutVersion(subKey []nibble, value Hasher, expected, version int64) {
	self.parentTree.SubPutVersion(self.key, subKey, value, self.version, expected, version)
}
func (self *subTreeWrapper) DelVersion(subKey []nibble, expected int64) {
	self.parentTree.SubDelVersion(self.key, subKey, self.version, expected)
}
func (self *subTreeWrapper) SubFinger(key, subKey []nibble, expected int64) (result *Print) {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubGetVersion(key, subKey []nibble, expected int64) (value Hasher, version int64, existed bool) {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubPutVersion(key, subKey []nibble, value Hasher, expected, subExpected, subVersion int64) {
	panic(subTreeError)
}
func (self *subTreeWrapper) SubDelVersion(key, subKey []nibble, expected, subExpected int64) {
	panic(subTreeError)
}

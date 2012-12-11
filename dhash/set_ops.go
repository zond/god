package dhash

import (
	"../client"
	"../common"
	"../radix"
)

const (
	bufferSize = 512
)

type setIter struct {
	key        []byte
	tree       *radix.Tree
	client     *client.Conn
	buffer     [][2][]byte
	nextIndex  int
	lastSubKey []byte
}

func (self *setIter) remoteRefill() {
	var items []common.Item
	if self.lastSubKey == nil {
		items = self.client.SliceLen(self.key, nil, true, bufferSize)
	} else {
		items = self.client.SliceLen(self.key, self.lastSubKey, false, bufferSize)
	}
	for _, item := range items {
		self.buffer = append(self.buffer, [2][]byte{item.Key, item.Value})
		self.lastSubKey = item.Key
	}
}

func (self *setIter) treeRefill() {
	filler := func(key, value []byte, timestamp int64) bool {
		self.buffer = append(self.buffer, [2][]byte{key, value})
		self.lastSubKey = key
		return len(self.buffer) < bufferSize
	}
	if self.lastSubKey == nil {
		self.tree.SubEachBetween(self.key, nil, nil, true, false, filler)
	} else {
		self.tree.SubEachBetween(self.key, self.lastSubKey, nil, false, false, filler)
	}
}

func (self *setIter) refill() bool {
	self.buffer = make([][2][]byte, 0, bufferSize)
	if self.tree == nil {
		self.remoteRefill()
	} else {
		self.treeRefill()
	}
	return len(self.buffer) > 0
}

func (self *setIter) next() (result [2][]byte, ok bool) {
	if self.nextIndex >= len(self.buffer) {
		if ok = self.refill(); !ok {
			return
		}
	}
	result = self.buffer[self.nextIndex]
	self.nextIndex++
	return
}

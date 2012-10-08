package shard

import (
	"bytes"
	"sort"
)

var board = newSwitchboard()

type Remote struct {
	Pos  []byte
	Addr string
}

func (self Remote) less(other Remote) bool {
	val := bytes.Compare(self.Pos, other.Pos)
	if val == 0 {
		val = bytes.Compare([]byte(self.Addr), []byte(other.Addr))
	}
	return val < 0
}
func (self Remote) call(service string, args, reply interface{}) error {
	return board.call(self.Addr, service, args, reply)
}

type Segment struct {
	Predecessor Remote
	Successor   Remote
}

func (self Segment) contains(position []byte) bool {
	return between(position, self.Predecessor.Pos, self.Successor.Pos)
}

type Ring struct {
	Nodes []Remote
}

func (self *Ring) size() int {
	return len(self.Nodes)
}
func (self *Ring) add(remote Remote) {
	for index, current := range self.Nodes {
		if current.Addr == remote.Addr {
			if bytes.Compare(current.Pos, remote.Pos) == 0 {
				return
			}
			self.Nodes = append(self.Nodes[:index], self.Nodes[index+1:]...)
		}
	}
	i := sort.Search(len(self.Nodes), func(i int) bool {
		return remote.less(self.Nodes[i])
	})
	if i < len(self.Nodes) {
		self.Nodes = append(self.Nodes[:i], append([]Remote{remote}, self.Nodes[i:]...)...)
	} else {
		self.Nodes = append(self.Nodes, remote)
	}
}
func (self *Ring) segmentIndices(leftPos, rightPos []byte) (predecessorIndex, successorIndex int) {
	i := sort.Search(len(self.Nodes), func(i int) bool {
		return bytes.Compare(rightPos, self.Nodes[i].Pos) < 0
	})
	if i < len(self.Nodes) {
		successorIndex = i
	} else {
		successorIndex = 0
	}
	startSearch := 0
	stopSearch := successorIndex
	if bytes.Compare(leftPos, rightPos) > 0 {
		startSearch = successorIndex
		stopSearch = len(self.Nodes)
	}
	j := sort.Search(stopSearch-startSearch, func(i int) bool {
		return bytes.Compare(leftPos, self.Nodes[i+startSearch].Pos) < 1
	})
	j += startSearch
	if j < len(self.Nodes) {
		if bytes.Compare(self.Nodes[j].Pos, leftPos) == 0 {
			predecessorIndex = j
		} else {
			if j > 0 {
				predecessorIndex = j - 1
			} else {
				predecessorIndex = len(self.Nodes) - 1
			}
		}
	} else {
		predecessorIndex = len(self.Nodes) - 1
	}
	return
}
func (self *Ring) clean(predecessor, successor []byte) {
	predecessorIndex, successorIndex := self.segmentIndices(predecessor, successor)
	if successorIndex > predecessorIndex {
		self.Nodes = append(self.Nodes[:predecessorIndex+1], self.Nodes[successorIndex-1:]...)
	} else {
		self.Nodes = self.Nodes[successorIndex-1 : predecessorIndex+1]
	}
}
func (self *Ring) segment(pos []byte) (result Segment) {
	predecessorIndex, successorIndex := self.segmentIndices(pos, pos)
	result.Predecessor = self.Nodes[predecessorIndex]
	result.Successor = self.Nodes[successorIndex]
	return
}

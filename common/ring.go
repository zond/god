package common

import (
	"../murmur"
	"bytes"
	"fmt"
	"math/big"
	"sort"
)

type Remote struct {
	Pos  []byte
	Addr string
}

func (self *Remote) EqualP(other *Remote) bool {
	if self == nil {
		if other == nil {
			return true
		}
		return false
	}
	if other == nil {
		return false
	}
	return (*self).Equal(*other)
}
func (self Remote) Equal(other Remote) bool {
	return self.Addr == other.Addr && bytes.Compare(self.Pos, other.Pos) == 0
}
func (self Remote) less(other Remote) bool {
	val := bytes.Compare(self.Pos, other.Pos)
	if val == 0 {
		val = bytes.Compare([]byte(self.Addr), []byte(other.Addr))
	}
	return val < 0
}
func (self Remote) String() string {
	return fmt.Sprintf("[%v@%v]", HexEncode(self.Pos), self.Addr)
}
func (self Remote) Call(service string, args, reply interface{}) error {
	return Switch.Call(self.Addr, service, args, reply)
}

type Ring struct {
	Nodes []Remote
}

func (self *Ring) Describe() string {
	buffer := new(bytes.Buffer)
	for index, node := range self.Nodes {
		fmt.Fprintf(buffer, "%v: %v\n", index, node)
	}
	return string(buffer.Bytes())
}
func (self *Ring) Size() int {
	return len(self.Nodes)
}
func (self *Ring) Equal(other *Ring) bool {
	if self == other {
		return true
	}
	if len(self.Nodes) != len(other.Nodes) {
		return false
	}
	for index, myNode := range self.Nodes {
		if !myNode.Equal(other.Nodes[index]) {
			return false
		}
	}
	return true
}
func (self *Ring) Add(remote Remote) {
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
func (self *Ring) Remotes(pos []byte) (before, at, after *Remote) {
	beforeIndex, atIndex, afterIndex := self.indices(pos)
	before = &self.Nodes[beforeIndex]
	if atIndex != -1 {
		at = &self.Nodes[atIndex]
	}
	after = &self.Nodes[afterIndex]
	return
}

/*
indices searches the ring for a position, and returns the last index before the position,
the index where the positon can be found (or -1) and the first index after the position.
*/
func (self *Ring) indices(pos []byte) (before, at, after int) {
	// Find the first position in self.Nodes where the position 
	// is greather than or equal to the searched for position.
	i := sort.Search(len(self.Nodes), func(i int) bool {
		return bytes.Compare(pos, self.Nodes[i].Pos) < 1
	})
	// If we didn't find any position like that
	if i == len(self.Nodes) {
		after = 0
		before = len(self.Nodes) - 1
		at = -1
		return
	}
	// If we did, then we know that the position before (or the last position) 
	// is the one that is before the searched for position.
	if i == 0 {
		before = len(self.Nodes) - 1
	} else {
		before = i - 1
	}
	// If we found a position that is equal to the searched for position 
	// just keep searching for a position that is guaranteed to be greater 
	// than the searched for position.
	// If we did not find a position that is equal, then we know that the found
	// position is greater than.
	cmp := bytes.Compare(pos, self.Nodes[i].Pos)
	if cmp == 0 {
		at = i
		j := sort.Search(len(self.Nodes)-i, func(k int) bool {
			return bytes.Compare(pos, self.Nodes[k+i].Pos) < 0
		})
		j += i
		if j < len(self.Nodes) {
			after = j
		} else {
			after = 0
		}
	} else {
		at = -1
		after = i
	}
	return
}
func (self *Ring) GetSlot() []byte {
	biggestSpace := new(big.Int)
	biggestSpaceIndex := 0
	for i := 0; i < len(self.Nodes); i++ {
		this := new(big.Int).SetBytes(self.Nodes[i].Pos)
		var next *big.Int
		if i+1 < len(self.Nodes) {
			next = new(big.Int).SetBytes(self.Nodes[i].Pos)
		} else {
			max := make([]byte, murmur.Size+1)
			max[0] = 1
			next = new(big.Int).Add(new(big.Int).SetBytes(max), new(big.Int).SetBytes(self.Nodes[0].Pos))
		}
		thisSpace := new(big.Int).Sub(next, this)
		if biggestSpace.Cmp(thisSpace) < 0 {
			biggestSpace = thisSpace
			biggestSpaceIndex = i
		}
	}
	return new(big.Int).Add(new(big.Int).SetBytes(self.Nodes[biggestSpaceIndex].Pos), new(big.Int).Div(biggestSpace, big.NewInt(2))).Bytes()
}
func (self *Ring) Remove(remote Remote) {
	for index, current := range self.Nodes {
		if current.Addr == remote.Addr {
			self.Nodes = append(self.Nodes[:index], self.Nodes[index+1:]...)
		}
	}
}
func (self *Ring) Clean(predecessor, successor []byte) {
	_, _, from := self.indices(predecessor)
	to, at, _ := self.indices(successor)
	if at != -1 {
		to = at
	}
	if from > to {
		self.Nodes = self.Nodes[to:from]
	} else {
		self.Nodes = append(self.Nodes[:from], self.Nodes[to:]...)
	}
}

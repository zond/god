package common

import (
	"../murmur"
	"bytes"
	"fmt"
	"sync"
	"math/big"
	"sort"
)


type ring struct {
	nodes []Remote
	lock *sync.RWMutex
}
func NewRing() *ring {
	return &ring{
		lock: new(sync.RWMutex),
	}
}
func NewRingNodes(nodes []Remote) *ring {
	return &ring{
		lock: new(sync.RWMutex),
		nodes: nodes,
	}
}

func (self *ring) Validate() {
	clone := self.Clone()
	seen := make(map[string]bool)
	for _, node := range clone.nodes {
		if _, ok := seen[node.Addr]; ok {
			panic(fmt.Errorf("duplicate node in ring! %v", clone.Describe()))
		}
		seen[node.Addr] = true
	}
}
func (self *ring) Describe() string {
	self.lock.RLock()
	defer self.lock.RUnlock()
	buffer := new(bytes.Buffer)
	for index, node := range self.nodes {
		fmt.Fprintf(buffer, "%v: %v\n", index, node)
	}
	return string(buffer.Bytes())
}
func (self *ring) Clone() *ring {
	self.lock.RLock()
	defer self.lock.RUnlock()
	nodes := make([]Remote, len(self.nodes))
	copy(nodes, self.nodes)
	return NewRingNodes(nodes)
}
func (self *ring) Size() int {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return len(self.nodes)
}
func (self *ring) Equal(other *ring) bool {
	if self == other {
		return true
	}
	clone := other.Clone()
	self.lock.RLock()
	defer self.lock.RUnlock()
	if len(self.nodes) != len(clone.nodes) {
		return false
	}
	for index, myNode := range self.nodes {
		if !myNode.Equal(clone.nodes[index]) {
			return false
		}
	}
	return true
}
func (self *ring) Add(remote Remote) {
	self.lock.Lock()
	defer self.lock.Unlock()
	for index, current := range self.nodes {
		if current.Addr == remote.Addr {
			if bytes.Compare(current.Pos, remote.Pos) == 0 {
				return
			}
			self.nodes = append(self.nodes[:index], self.nodes[index+1:]...)
		}
	}
	i := sort.Search(len(self.nodes), func(i int) bool {
		return remote.less(self.nodes[i])
	})
	if i < len(self.nodes) {
		self.nodes = append(self.nodes[:i], append([]Remote{remote}, self.nodes[i:]...)...)
	} else {
		self.nodes = append(self.nodes, remote)
	}
}
func (self *ring) Redundancy() int {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if len(self.nodes) < Redundancy {
		return len(self.nodes)
	}
	return Redundancy
}
func (self *ring) Remotes(pos []byte) (before, at, after *Remote) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	beforeIndex, atIndex, afterIndex := self.indices(pos)
	if beforeIndex != -1 {
		before = &self.nodes[beforeIndex]
	}
	if atIndex != -1 {
		at = &self.nodes[atIndex]
	}
	if afterIndex != -1 {
		after = &self.nodes[afterIndex]
	}
	return
}

/*
indices searches the ring for a position, and returns the last index before the position,
the index where the positon can be found (or -1) and the first index after the position.
*/
func (self *ring) indices(pos []byte) (before, at, after int) {
	if len(self.nodes) == 0 {
		return -1, -1, -1
	}
	// Find the first position in self.nodes where the position 
	// is greather than or equal to the searched for position.
	i := sort.Search(len(self.nodes), func(i int) bool {
		return bytes.Compare(pos, self.nodes[i].Pos) < 1
	})
	// If we didn't find any position like that
	if i == len(self.nodes) {
		after = 0
		before = len(self.nodes) - 1
		at = -1
		return
	}
	// If we did, then we know that the position before (or the last position) 
	// is the one that is before the searched for position.
	if i == 0 {
		before = len(self.nodes) - 1
	} else {
		before = i - 1
	}
	// If we found a position that is equal to the searched for position 
	// just keep searching for a position that is guaranteed to be greater 
	// than the searched for position.
	// If we did not find a position that is equal, then we know that the found
	// position is greater than.
	cmp := bytes.Compare(pos, self.nodes[i].Pos)
	if cmp == 0 {
		at = i
		j := sort.Search(len(self.nodes)-i, func(k int) bool {
			return bytes.Compare(pos, self.nodes[k+i].Pos) < 0
		})
		j += i
		if j < len(self.nodes) {
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
func (self *ring) GetSlot() []byte {
	self.lock.RLock()
	defer self.lock.RUnlock()
	biggestSpace := new(big.Int)
	biggestSpaceIndex := 0
	for i := 0; i < len(self.nodes); i++ {
		this := new(big.Int).SetBytes(self.nodes[i].Pos)
		var next *big.Int
		if i+1 < len(self.nodes) {
			next = new(big.Int).SetBytes(self.nodes[i].Pos)
		} else {
			max := make([]byte, murmur.Size+1)
			max[0] = 1
			next = new(big.Int).Add(new(big.Int).SetBytes(max), new(big.Int).SetBytes(self.nodes[0].Pos))
		}
		thisSpace := new(big.Int).Sub(next, this)
		if biggestSpace.Cmp(thisSpace) < 0 {
			biggestSpace = thisSpace
			biggestSpaceIndex = i
		}
	}
	return new(big.Int).Add(new(big.Int).SetBytes(self.nodes[biggestSpaceIndex].Pos), new(big.Int).Div(biggestSpace, big.NewInt(2))).Bytes()
}
func (self *ring) Remove(remote Remote) {
	self.lock.Lock()
	defer self.lock.Unlock()
	for index, current := range self.nodes {
		if current.Addr == remote.Addr {
			if len(self.nodes) == 1 {
				panic("Why would you want to remove the last Node in the ring? Inconceivable!")
			}
			self.nodes = append(self.nodes[:index], self.nodes[index+1:]...)
		}
	}
}
func (self *ring) Clean(predecessor, successor []byte) {
	self.lock.Lock()
	defer self.lock.Unlock()
	_, _, from := self.indices(predecessor)
	to, at, _ := self.indices(successor)
	if at != -1 {
		to = at
	}
	if from > to {
		self.nodes = self.nodes[to:from]
	} else {
		self.nodes = append(self.nodes[:from], self.nodes[to:]...)
	}
}

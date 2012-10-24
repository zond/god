package common

import (
	"../murmur"
	"bytes"
	"fmt"
	"sync"
	"math/big"
	"sort"
)


type Ring struct {
	nodes Remotes
	lock *sync.RWMutex
}
func NewRing() *Ring {
	return &Ring{
		lock: new(sync.RWMutex),
	}
}
func NewRingNodes(nodes Remotes) *Ring {
	return &Ring{
		lock: new(sync.RWMutex),
		nodes: nodes,
	}
}

func (self *Ring) Validate() {
	clone := self.Clone()
	seen := make(map[string]bool)
	var last *Remote
	for _, node := range clone.nodes {
		if _, ok := seen[node.Addr]; ok {
			panic(fmt.Errorf("Duplicate node in Ring! %v", clone.Describe()))
		}
		if last != nil && node.Less(*last) {
			panic(fmt.Errorf("Badly ordered Ring! %v", clone.Describe()))
		}
		last = &node
		seen[node.Addr] = true
	}
}
func (self *Ring) Describe() string {
	self.lock.RLock()
	defer self.lock.RUnlock()
	buffer := new(bytes.Buffer)
	for index, node := range self.nodes {
		fmt.Fprintf(buffer, "%v: %v\n", index, node)
	}
	return string(buffer.Bytes())
}
func (self *Ring) SetNodes(nodes Remotes) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.nodes = make(Remotes, len(nodes))
	copy(self.nodes, nodes)
}
func (self *Ring) Nodes() (nodes Remotes) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	nodes = make(Remotes, len(self.nodes))
	copy(nodes, self.nodes)
	return
}
func (self *Ring) Clone() *Ring {
	return NewRingNodes(self.Nodes())
}
func (self *Ring) Size() int {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return len(self.nodes)
}
func (self *Ring) Equal(other *Ring) bool {
	return self.Nodes().Equal(other.Nodes())
}
func (self *Ring) Add(remote Remote) {
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
		return remote.Less(self.nodes[i])
	})
	if i < len(self.nodes) {
		self.nodes = append(self.nodes[:i], append(Remotes{remote}, self.nodes[i:]...)...)
	} else {
		self.nodes = append(self.nodes, remote)
	}
}
func (self *Ring) Redundancy() int {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if len(self.nodes) < Redundancy {
		return len(self.nodes)
	}
	return Redundancy
}
func (self *Ring) Remotes(pos []byte) (before, at, after *Remote) {
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
indices searches the Ring for a position, and returns the last index before the position,
the index where the positon can be found (or -1) and the first index after the position.
*/
func (self *Ring) indices(pos []byte) (before, at, after int) {
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
func (self *Ring) GetSlot() []byte {
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
func (self *Ring) Remove(remote Remote) {
	self.lock.Lock()
	defer self.lock.Unlock()
	for index, current := range self.nodes {
		if current.Addr == remote.Addr {
			if len(self.nodes) == 1 {
				panic("Why would you want to remove the last Node in the Ring? Inconceivable!")
			}
			self.nodes = append(self.nodes[:index], self.nodes[index+1:]...)
		}
	}
}
func (self *Ring) Clean(predecessor, successor []byte) {
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

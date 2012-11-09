package common

import (
	"../murmur"
	"bytes"
	"fmt"
	"math/big"
	"math/rand"
	"sort"
	"sync"
)

type RingChangeListener func(ring *Ring)

type Ring struct {
	nodes           Remotes
	lock            *sync.RWMutex
	changeListeners []RingChangeListener
}

func NewRing() *Ring {
	return &Ring{
		lock: new(sync.RWMutex),
	}
}
func NewRingNodes(nodes Remotes) *Ring {
	return &Ring{
		lock:  new(sync.RWMutex),
		nodes: nodes,
	}
}

func (self *Ring) AddChangeListener(f RingChangeListener) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.changeListeners = append(self.changeListeners, f)
}
func (self *Ring) Random() Remote {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.nodes[rand.Int()%len(self.nodes)].Clone()
}
func (self *Ring) hash() []byte {
	hasher := murmur.New()
	for _, node := range self.nodes {
		hasher.MustWrite(node.Pos)
		hasher.MustWrite([]byte(node.Addr))
	}
	return hasher.Get()
}
func (self *Ring) Hash() []byte {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.hash()
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
	return self.nodes.Describe()
}
func (self *Ring) SetNodes(nodes Remotes) {
	self.lock.Lock()
	defer self.lock.Unlock()
	h := self.hash()
	self.nodes = nodes.Clone()
	self.sendChanges(h)
}
func (self *Ring) sendChanges(oldHash []byte) {
	if bytes.Compare(oldHash, self.hash()) != 0 {
		clone := NewRingNodes(self.nodes.Clone())
		for _, listener := range self.changeListeners {
			self.lock.Unlock()
			listener(clone)
			self.lock.Lock()
		}
	}
}
func (self *Ring) Nodes() Remotes {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.nodes.Clone()
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
func (self *Ring) Add(r Remote) {
	self.lock.Lock()
	defer self.lock.Unlock()
	oldHash := self.hash()
	remote := r.Clone()
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
	self.sendChanges(oldHash)
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
		tmp := self.nodes[beforeIndex].Clone()
		before = &tmp
	}
	if atIndex != -1 {
		tmp := self.nodes[atIndex].Clone()
		at = &tmp
	}
	if afterIndex != -1 {
		tmp := self.nodes[afterIndex].Clone()
		after = &tmp
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
			next = new(big.Int).SetBytes(self.nodes[i+1].Pos)
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
	oldHash := self.hash()
	for index, current := range self.nodes {
		if current.Addr == remote.Addr {
			if len(self.nodes) == 1 {
				panic("Why would you want to remove the last Node in the Ring? Inconceivable!")
			}
			self.nodes = append(self.nodes[:index], self.nodes[index+1:]...)
		}
	}
	self.sendChanges(oldHash)
}
func (self *Ring) Clean(predecessor, successor []byte) {
	self.lock.Lock()
	defer self.lock.Unlock()
	oldHash := self.hash()
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
	self.sendChanges(oldHash)
}

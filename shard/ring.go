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

type Surrounding struct {
	Predecessor Remote
	Successor   Remote
}

func (self Surrounding) contains(position []byte) bool {
	return between(position, self.Predecessor.Pos, self.Successor.Pos)
}

type Ring struct {
	Nodes []Remote
}

func (self *Ring) add(remote Remote) {
	for index, current := range self.Nodes {
		if current.Addr == remote.Addr {
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
func (self *Ring) surrounding(pos []byte) (result Surrounding) {
	i := sort.Search(len(self.Nodes), func(i int) bool {
		return bytes.Compare(pos, self.Nodes[i].Pos) < 0
	})
	if i < len(self.Nodes) {
		result.Successor = self.Nodes[i]
	} else {
		result.Successor = self.Nodes[0]
	}
	j := sort.Search(i, func(i int) bool {
		return bytes.Compare(pos, self.Nodes[i].Pos) < 1
	})
	if j < len(self.Nodes) {
		if bytes.Compare(self.Nodes[j].Pos, pos) == 0 {
			result.Predecessor = self.Nodes[j]
		} else {
			if j > 0 {
				result.Predecessor = self.Nodes[j-1]
			} else {
				result.Predecessor = self.Nodes[len(self.Nodes)-1]
			}
		}
	} else {
		result.Predecessor = self.Nodes[len(self.Nodes)-1]
	}
	return
}

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

type Remotes struct {
	Content []Remote
}

func (self *Remotes) add(remote Remote) {
	for index, current := range self.Content {
		if current.Addr == remote.Addr {
			self.Content = append(self.Content[:index], self.Content[index+1:]...)
		}
	}
	i := sort.Search(len(self.Content), func(i int) bool {
		return remote.less(self.Content[i])
	})
	if i < len(self.Content) {
		self.Content = append(self.Content[:i], append([]Remote{remote}, self.Content[i:]...)...)
	} else {
		self.Content = append(self.Content, remote)
	}
}
func (self *Remotes) surrounding(pos []byte) (result Surrounding) {
	i := sort.Search(len(self.Content), func(i int) bool {
		return bytes.Compare(pos, self.Content[i].Pos) < 0
	})
	if i < len(self.Content) {
		result.Successor = self.Content[i]
	} else {
		result.Successor = self.Content[0]
	}
	j := sort.Search(i, func(i int) bool {
		return bytes.Compare(pos, self.Content[i].Pos) < 1
	})
	if j < len(self.Content) {
		if bytes.Compare(self.Content[j].Pos, pos) == 0 {
			result.Predecessor = self.Content[j]
		} else {
			if j > 0 {
				result.Predecessor = self.Content[j-1]
			} else {
				result.Predecessor = self.Content[len(self.Content)-1]
			}
		}
	} else {
		result.Predecessor = self.Content[len(self.Content)-1]
	}
	return
}

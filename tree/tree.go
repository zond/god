
package tree

import (
	"bytes"
	"math/rand"
	"fmt"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Thing interface{}

type TreeIterator func(key []byte, value Thing) bool

type direction int

const (
	Down = 0
	Up = 1
)

type Tree struct {
	size int
	root *node
}
func (self *Tree) Put(key []byte, value Thing) (old Thing, existed bool) {
	self.root, existed, old = self.root.insert(newNode(key, value))
	if !existed {
		self.size++
	}
	return
}
func (self *Tree) Get(key []byte) (value Thing, existed bool) {
	return self.root.get(key)
}
func (self *Tree) Del(key []byte) (old Thing, existed bool) {
	self.root, existed, old = self.root.del(key)
	if existed {
		self.size--
	}
	return
}
func (self *Tree) Each(dir direction, from []byte, f TreeIterator) {
	self.root.each(dir, from, f)
}
func (self *Tree) Size() int {
	return self.size
}
func (self *Tree) String() string {
	return fmt.Sprint(self.ToMap())
}
func (self *Tree) ToMap() map[string]Thing {
	rval := make(map[string]Thing)
	self.Each(Up, []byte{}, func(key []byte, value Thing) bool {
		fmt.Println("key: ", string(key), " value: ", value)
		rval[string(key)] = value
		return true
	})
	return rval
}

func merge(left, right *node) (result *node) {
	if left == nil {
		result = right
	} else if right == nil {
		result = left
	} else if left.weight < right.weight {
		result, left.right = left, merge(left.right, right)
	} else {
		result, right.left = right, merge(right.left, left)
	}
	return
}

type node struct {
	weight int32
	left *node
	right *node
	key []byte
	value Thing
}
func newNode(key []byte, value Thing) *node {
	return &node{
		weight: rand.Int31(),
		key: key,
		value: value,
	}
}
func (self *node) get(key []byte) (value Thing, existed bool) {
	if self != nil {
		switch bytes.Compare(key, self.key) {
		case -1:
			return self.left.get(key)
		case 1:
			return self.right.get(key)
		default:
			value, existed = self.value, true
		}
		
	}
	return
}
func (self *node) each(dir direction, from []byte, f TreeIterator) {
	if self != nil {
		cmp := bytes.Compare(from, self.key)
		order := []*node{self.left, self.right}
		if dir == Down {
			order = []*node{self.right, self.left}
			cmp = cmp * -1
		}
		switch cmp {
		case -1:
			order[0].each(dir, from, f)
			if f(self.key, self.value) {
				order[1].each(dir, from, f)
			}
		case 1:
			order[1].each(dir, from, f)
		default:
			if f(self.key, self.value) {
				order[1].each(dir, from, f)
			}
		}
	}
}
func (self *node) rotateLeft() (result *node) {
	result, self.left = self.left, self.left.right
	return
}
func (self *node) rotateRight() (result *node) {
	result, self.right = self.right, self.right.left
	return
}
func (self *node) del(key []byte) (result *node, existed bool, old Thing) {
	if self != nil {
		result = self
		switch bytes.Compare(key, self.key) {
		case -1:
			self.left, existed, old = self.left.del(key)
		case 1:
			self.right, existed, old = self.right.del(key)
		default:
			result, existed, old = merge(self.left, self.right), true, self.value
		}
	}
	return
}
func (self *node) insert(n *node) (result *node, existed bool, old Thing) {
	result = n
	if self != nil {
		result = self
		switch bytes.Compare(n.key, self.key) {
		case -1:
			self.left, existed, old = self.left.insert(n)
			if self.left.weight < self.weight {
				result = self.rotateLeft()
			}
		case 1:
			self.right, existed, old = self.right.insert(n)
			if self.right.weight < self.weight {
				result = self.rotateRight()
			}
		default:
			existed, old, self.value = true, self.value, n.value
		}
	}
	return
}
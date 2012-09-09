
package tree

import (
	"bytes"
	"math/rand"
	"fmt"
)

type Thing interface{}

type TreeIterator func(key []byte, value Thing) bool


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
func (self *Tree) Del(key []byte) (old Thing, existed bool) {
	self.root, existed, old = self.root.del(key)
	if existed {
		self.size--
	}
	return
}
func (self *Tree) Each(dir int, f TreeIterator) {
	self.root.each(dir, f)
}
func (self *Tree) Size() int {
	return self.size
}
func (self *Tree) String() string {
	return fmt.Sprint(self.ToMap())
}
func (self *Tree) ToMap() map[string]Thing {
	rval := make(map[string]Thing)
	self.Each(1, func(key []byte, value Thing) bool {
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
func (self *node) each(dir int, f TreeIterator) {
	if self != nil {
		order := []*node{self.left, self.right}
		if dir < 0 {
			order = []*node{self.right, self.left}
		}
		order[0].each(dir, f)
		if f(self.key, self.value) {
			order[1].each(dir, f)
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
	if self == nil {
		return
	} 
	result = self
	switch bytes.Compare(self.key, key) {
	case -1:
		self.left, existed, old = self.left.del(key)
	case 1:
		self.right, existed, old = self.right.del(key)
	default:
		result, existed, old = merge(self.left, self.right), true, self.value
	}
	return
}
func (self *node) insert(n *node) (result *node, existed bool, old Thing) {
	if self == nil {
		result = n
		return
	}
	result = self
	switch bytes.Compare(self.key, n.key) {
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
	return
}
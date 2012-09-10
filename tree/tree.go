
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

type TreeIterator func(key []byte, value Thing)

type direction int

const (
	Down = iota
	Up
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
func (self *Tree) Describe() string {
	buffer := bytes.NewBufferString(fmt.Sprintf("<Tree size:%v>\n", self.Size()))
	self.root.describe(0, buffer)
	return string(buffer.Bytes())
}
func (self *Tree) Del(key []byte) (old Thing, existed bool) {
	self.root, existed, old = self.root.del(key)
	if existed {
		self.size--
	}
	return
}
func (self *Tree) Each(dir direction, min, max []byte, f TreeIterator) {
	self.root.each(dir, min, max, f)
}
func (self *Tree) Size() int {
	return self.size
}
func (self *Tree) String() string {
	return fmt.Sprint(self.ToMap())
}
func (self *Tree) ToMap() map[string]Thing {
	rval := make(map[string]Thing)
	self.Each(Up, nil, nil, func(key []byte, value Thing) {
		rval[string(key)] = value
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
func (self *node) describe(indent int, buffer *bytes.Buffer) {
	if self != nil {
		self.left.describe(indent + 1, buffer)
		indentation := &bytes.Buffer{}
		for i := 0; i < indent; i++ {
			fmt.Fprint(indentation, " ")
		}
		fmt.Fprintf(buffer, "%v%v [%v] => %v\n", string(indentation.Bytes()), self.key, self.weight, self.value)
		self.right.describe(indent + 1, buffer)
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
func (self *node) each(dir direction, min, max []byte, f TreeIterator) {
	if self != nil {
		min_cmp := -1
		if min != nil {
			min_cmp = bytes.Compare(min, self.key)
		}
		max_cmp := 1
		if max != nil {
			max_cmp = bytes.Compare(max, self.key)
		}

		order := []*node{self.left, self.right}
		if dir == Down {
			order = []*node{self.right, self.left}
			min_cmp, max_cmp = max_cmp * -1, min_cmp * -1
		}

		if min_cmp < 0 {
			order[0].each(dir, min, max, f)
		}
		if min_cmp < 1 && max_cmp > -1 {
			f(self.key, self.value)
		}
		if max_cmp > 0 {
			order[1].each(dir, min, max, f)
		}
	}
}
func (self *node) rotateLeft() (result *node) {
	result, self.left, self.left.right = self.left, self.left.right, self
	return
}
func (self *node) rotateRight() (result *node) {
	result, self.right, self.right.left = self.right, self.right.left, self
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
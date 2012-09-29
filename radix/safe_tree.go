package radix

import (
	"sync"
)

type SafeTree struct {
	tree *Tree
	lock *sync.RWMutex
}

func NewSafeTree() *SafeTree {
	return &SafeTree{NewTree(), &sync.RWMutex{}}
}
func (self *SafeTree) Finger(key []byte) *Print {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.tree.Finger(key)
}
func (self *SafeTree) Put(key []byte, value Hasher) (Hasher, bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	return self.tree.Put(key, value)
}
func (self *SafeTree) PutVersion(key []byte, value Hasher, version uint32) (Hasher, uint32, bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	return self.tree.PutVersion(key, value, version)
}
func (self *SafeTree) Hash() []byte {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.tree.Hash()
}
func (self *SafeTree) Get(key []byte) (Hasher, bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.tree.Get(key)
}
func (self *SafeTree) GetVersion(key []byte) (Hasher, uint32, bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.tree.GetVersion(key)
}
func (self *SafeTree) Del(key []byte) (Hasher, bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	return self.tree.Del(key)
}
func (self *SafeTree) ToMap() map[string]Hasher {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.tree.ToMap()
}
func (self *SafeTree) Each(f TreeIterator) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.tree.Each(f)
}
func (self *SafeTree) Size() int {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.tree.Size()
}
func (self *SafeTree) Describe() string {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.tree.Describe()
}

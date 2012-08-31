
package shard

import (
	"sync"
)

type Map struct {
	content map[string]string
	lock *sync.RWMutex
}
func NewMap() *Map {
	return &Map{make(map[string]string), new(sync.RWMutex)}
}
func (self *Map) Get(k string) (string, bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	v, ok := self.content[k]
	return v, ok
}
func (self *Map) Del(k string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	delete(self.content, k)
}
func (self *Map) DelIfPresent(k, exp string) bool {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if v, ok := self.content[k]; ok && v == exp {
		self.lock.Lock()
		defer self.lock.Unlock()
		delete(self.content, k)
		return true
	}
	return false	
}
func (self *Map) PutIfPresent(k, v, exp string) bool {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if v, ok := self.content[k]; ok && v == exp{
		self.lock.Lock()
		defer self.lock.Unlock()
		self.content[k] = v
		return true
	}
	return false
}
func (self *Map) PutIfMissing(k, v string) bool {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if v, ok := self.content[k]; !ok {
		self.lock.Lock()
		defer self.lock.Unlock()
		self.content[k] = v
		return true
	}
	return false
}
func (self *Map) Put(k, v string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.content[k] = v
}
func (self *Map) Keys() []string {
	self.lock.RLock()
	defer self.lock.RUnlock()
	rval := make([]string, len(self.content))
	index := 0
	for key, _ := range self.content {
		rval[index] = key
		index++
	}
	return rval
}
func (self *Map) Size() int {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return len(self.content)
}


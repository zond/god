
package shard

import (
	"sync"
)

type rwMap struct {
	content map[string]string
	lock *sync.RWMutex
}
func newRwMap() *rwMap {
	return &rwMap{make(map[string]string), new(sync.RWMutex)}
}
func (self *rwMap) get(k string) (string, bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	v, ok := self.content[k]
	return v,ok
}
func (self *rwMap) del(k string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	delete(self.content, k)
}
func (self *rwMap) delIfPresent(k, exp string) bool {
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
func (self *rwMap) putIfPresent(k, v, exp string) bool {
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
func (self *rwMap) putIfMissing(k, v string) bool {
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
func (self *rwMap) put(k, v string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.content[k] = v
}
func (self *rwMap) keys() []string {
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
func (self *rwMap) size() int {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return len(self.content)
}



package shard

import (
	"sync"
)

type semaphore sync.Cond

func newSemaphore() *semaphore {
	return (*semaphore)(sync.NewCond(&sync.Mutex{}))
}
func (self *semaphore) wait() {
	self.L.Lock()
	(*sync.Cond)(self).Wait()
	self.L.Unlock()
}
func (self *semaphore) broadcast() {
	(*sync.Cond)(self).Broadcast()
}


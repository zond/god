
package shard

import (
	"github.com/zond/gotomic"
	"fmt"
	"runtime"
	"os"
	"sync/atomic"
)

type Shard struct {
	hash *gotomic.Hash
	dir string
	logChannel chan loggedOperation
	masterSnapshot chan Operation
	masterStream chan Operation
	masterStreamSem *semaphore
	slaveChannel chan slave
	restoring bool
	snapshotting int32
	closed int32
	conf map[string]interface{}
}
func NewShard(dir string) (*Shard, error) {
	rval := &Shard{
		hash: gotomic.NewHash(), 
		dir: dir, 
		logChannel: make(chan loggedOperation), 
		slaveChannel: make(chan slave),
		conf: make(map[string]interface{}),
	}
	runtime.SetFinalizer(rval, func(s *Shard) {
		s.Close()
	})
	if err := os.MkdirAll(rval.dir, 0700); err != nil {
		panic(fmt.Errorf("When trying to create %v for %v: %v", rval.dir, rval, err))
	}
	go rval.store()
	rval.load()
	return rval, nil
}
func (self *Shard) Close() {
	self.stopSlavery()
	self.closeLogs()
	atomic.StoreInt32(&self.closed, 1)
}
func NewEmptyShard(dir string) (*Shard, error) {
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	rval, err := NewShard(dir)
	if err != nil {
		return nil, err
	}
	return rval, nil
}

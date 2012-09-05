
package shard

import (
	"github.com/zond/gotomic"
	"fmt"
	"runtime"
	"os"
)

type Shard struct {
	hash *gotomic.Hash
	dir string
	logChannel chan loggedOperation
	slaveChannel chan slave
	restoring bool
	snapshotting int32
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
	if err := os.MkdirAll(rval.dir, 0700); err != nil {
		panic(fmt.Errorf("When trying to create %v for %v: %v", rval.dir, rval, err))
	}
	go rval.store()
	runtime.SetFinalizer(rval, func(s *Shard) {
		close(s.logChannel)
	})
	rval.load()
	return rval, nil
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

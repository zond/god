
package shard

import (
	"fmt"
	"strconv"
	"time"
	"path/filepath"
	"encoding/gob"
)

func (self *Shard) setMaster(snapshot, stream chan Operation) {
	sem := newSemaphore()
	go self.bufferMaster(stream, sem)
	response := &Response{}
	self.Perform(Operation{CLEAR, []string{}}, response)
	if response.Result & OK != OK {
		panic(fmt.Errorf("When trying to clear: %v", response))
	}
	go self.followMaster(snapshot, sem)
}
func (self *Shard) getOldestFollow() (path string, t time.Time) {
	for _, log := range self.getLogs() {
		if followPattern.MatchString(log) {
			tmp, _ := strconv.ParseInt(followPattern.FindStringSubmatch(log)[1], 10, 64)
			t = time.Unix(0, tmp)
			path = log
			return
		}
	}
	panic(fmt.Errorf("There doesn't seem to be any follow logs!"))
}
func (self *Shard) getNextFollow(after time.Time) (path string, t time.Time, ok bool) {
	for _, log := range self.getLogs() {
		if followPattern.MatchString(log) {
			tmp, _ := strconv.ParseInt(followPattern.FindStringSubmatch(log)[1], 10, 64)
			this_t := time.Unix(0, tmp)
			if this_t.After(after) {
				path = log
				ok = true
				t = this_t
				return
			}
		}
	}
	return
}
func (self *Shard) followMaster(snapshot chan Operation, sem *semaphore) {
	response := &Response{}
	for op := range snapshot {
		self.Perform(op, response)
		if response.Result & OK != OK {
			panic(fmt.Errorf("While trying to perform %v: %v", op, response))
		}
	}
	path, t := self.getOldestFollow()
	decoder := newDecoderFile(filepath.Join(self.dir, path))
	for {
		self.loadDecoder(decoder)
		next_path, next_t, ok := self.getNextFollow(t)
		if ok {
			path = next_path
			t = next_t
			decoder = newDecoderFile(filepath.Join(self.dir, path))
		} else {
			sem.wait()
		}
	}
}
func (self *Shard) bufferMaster(stream chan Operation, sem *semaphore) {
	logfile := self.newLogFile(time.Now(), followFormat)
	encoder := gob.NewEncoder(logfile)
	for op := range stream {
		if err := encoder.Encode(op); err != nil {
			panic(fmt.Errorf("While trying to log %v: %v", op, err))
		}
		sem.broadcast()
		if self.tooLargeLog(logfile) {
			logfile.Close()
			logfile = self.newLogFile(time.Now(), followFormat)
			encoder = gob.NewEncoder(logfile)
		}
	}
}


package shard

import (
	"path/filepath"
	"strconv"
	"encoding/gob"
	"io"
	"time"
	"github.com/zond/gotomic"
	"sync/atomic"
	"fmt"
	"os"
)

const (
	snapshotFormat = "snapshot-%v.log"
	streamFormat = "stream-%v.log"
	followFormat = "follow-%v.log"
	maxLogSize = "maxLogSize"
	defaultMaxLogSize = 1024 * 1024 * 128
)

type slave struct {
	snapshot chan Operation
	stream chan Operation
}

type loggedOperation struct {
	operation Operation
	done chan bool
}

func (self *Shard) loadDecoder(decoder *fileDecoder) {
	operation := Operation{}
	response := Response{}
	err := decoder.Decode(&operation)
	for err == nil {
		self.Perform(operation, &response)
		if response.Result & OK != OK {
			panic(fmt.Errorf("Trying to perform %v resulted in %v", operation, response))
		}
		err = decoder.Decode(&operation)
	}
	if err != io.EOF {
		panic(fmt.Errorf("While trying to load %v: %v", decoder, err))
	}
}
func (self *Shard) stopLogging() {
	close(self.slaveChannel)
	close(self.logChannel)
}
func (self *Shard) isClosed() bool {
	return atomic.LoadInt32(&self.closed) == 1
}
func (self *Shard) load() {
	self.restoring = true
	latestSnapshot, snapshotTime, snapshotFound := self.getLastSnapshot()
	if snapshotFound {
		self.loadDecoder(newFileDecoder(filepath.Join(self.dir, latestSnapshot)))
	}
	for _, stream := range self.getStreamsAfter(snapshotTime) {
	        self.loadDecoder(newFileDecoder(filepath.Join(self.dir, stream)))
        }
	self.restoring = false
}
func (self *Shard) snapshot(t time.Time) {
	if atomic.CompareAndSwapInt32(&self.snapshotting, 0, 1) {
		defer atomic.StoreInt32(&self.snapshotting, 0)

		filename := filepath.Join(self.dir, fmt.Sprintf(snapshotFormat, t.UnixNano()))
		tmpfilename := fmt.Sprint(filename, ".spool")
		snapshot, err := os.Create(tmpfilename)
		if err != nil {
			panic(fmt.Errorf("While trying to create %v: %v", tmpfilename, err))
		}
		encoder := gob.NewEncoder(snapshot)
		self.hash.Each(func(k gotomic.Hashable, v gotomic.Thing) bool {
			if self.isClosed() {
				return false
			}
			op := Operation{PUT, []string{string(k.(gotomic.StringKey)), v.(string)}}
			if err = encoder.Encode(op); err != nil {
				panic(fmt.Errorf("While trying to encode %v: %v", op, err))
			}
			return true
		}) 
		snapshot.Close()
		if err = os.Rename(tmpfilename, filename); err == nil {
			for _, log := range self.getLogs(logPattern) {
				tmp, _ := strconv.ParseInt(logPattern.FindStringSubmatch(log)[1], 10, 64)
				logtime := time.Unix(0, tmp)
				if logtime.Before(t) {
					os.Remove(filepath.Join(self.dir, log))
				}
			}
		}
	}
}
func (self *Shard) flushSnapshot(log chan Operation) {
	self.hash.Each(func(k gotomic.Hashable, v gotomic.Thing) bool {
		if self.isClosed() {
			return false
		}
		log <- Operation{PUT, []string{string(k.(gotomic.StringKey)), v.(string)}}
		return true
	});
	close(log)
}
func (self *Shard) store() {
	slaves := make(map[slave]bool)
	logfile := self.newLogFile(time.Now(), streamFormat)
	encoder := gob.NewEncoder(logfile)
	for {
		select {
		case entry, ok := <- self.logChannel:
			if !ok {
				self.cleanSlaves(slaves)
				return
			}
			if entry.operation.Command == CLEAR {
				logfile.Close()
				for _, log := range self.getLogs(logPattern) {
					if err := os.Remove(filepath.Join(self.dir, log)); err != nil {
						panic(fmt.Errorf("While trying to clear %v: %v", self, err))
					}
				}
				self.hash = gotomic.NewHash()
				logfile = self.newLogFile(time.Now(), streamFormat)
				encoder = gob.NewEncoder(logfile)
			} else {
				if err := encoder.Encode(entry.operation); err != nil {
					panic(fmt.Errorf("While trying to log %v: %v", entry.operation, err))
				}
				if self.tooLargeLog(logfile) {
					logfile.Close()
					t := time.Now()
					go self.snapshot(t)
					logfile = self.newLogFile(t, streamFormat)
					encoder = gob.NewEncoder(logfile)
				}
			}
			for slave, _ := range slaves {
				slave.stream <- entry.operation
			}
			if entry.done != nil {
				entry.done <- true
			}
		case slave, ok := <- self.slaveChannel:
			if !ok {
				self.cleanSlaves(slaves)
				return
			}
			slaves[slave] = true
			go self.flushSnapshot(slave.snapshot)
		}
		select {
		case slave, ok := <- self.slaveChannel:
			if !ok {
				self.cleanSlaves(slaves)
				return
			}
			slaves[slave] = true
			go self.flushSnapshot(slave.snapshot)
		default:
		}
	}
	logfile.Close()
}
func (self *Shard) cleanSlaves(slaves map[slave]bool) {
	for slave, _ := range slaves {
		close(slave.snapshot)
		close(slave.stream)
	}
}
func (self *Shard) log(o Operation, done chan bool) {
	if !self.restoring {
		self.logChannel <- loggedOperation{o, done}
	}
}

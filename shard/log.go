
package shard

import (
	"regexp"
	"path/filepath"
	"strconv"
	"encoding/gob"
	"io"
	"time"
	"sort"
	"github.com/zond/gotomic"
	"sync/atomic"
	"fmt"
	"os"
)

const (
	snapshot = "snapshot-%v.log"
	streamFormat = "stream-%v.log"
	maxLogSize = "maxLogSize"
	defaultMaxLogSize = 1024 * 1024 * 128
)

var streamPattern = regexp.MustCompile("^stream-(\\d+)\\.log$")
var snapshotPattern = regexp.MustCompile("^snapshot-(\\d+)\\.log$")
var logPattern = regexp.MustCompile("^\\w+-(\\d+)\\.log$")

type loggedOperation struct {
	operation Operation
	done chan bool
}

type logNames []string
func (self logNames) Len() int {
	return len(self)
}
func (self logNames) Less(i, j int) bool {
	vi, _ := strconv.ParseUint(logPattern.FindString(self[i]), 10, 64)
	vj, _ := strconv.ParseUint(logPattern.FindString(self[j]), 10, 64)
	return vi < vj
}
func (self logNames) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self *Shard) loadPath(path string) error {
	file, err := os.Open(path)
	if err != nil {
		panic(fmt.Errorf("While trying to load %v for %v: %v", path, self, err))
	}
	defer file.Close()
	decoder := gob.NewDecoder(file)
	operation := Operation{}
	response := Response{}
	err = decoder.Decode(&operation)
	for err == nil {
		self.Perform(operation, &response)
		err = decoder.Decode(&operation)
	}
	if err != io.EOF {
		panic(fmt.Errorf("While trying to load %v for %v: %v", path, self, err))
	}
	return nil
}
func (self *Shard) getLastSnapshot() (filename string, ok bool, t time.Time) {
        directory, err := os.Open(self.dir)
        if err != nil {
		panic(fmt.Errorf("While trying to find last snapshot for %v: %v", self, err))
        }
        children, err := directory.Readdirnames(-1)
        if err != nil {
		panic(fmt.Errorf("While trying to find last snapshot for %v: %v", self, err))
        }
	sort.Sort(logNames(children))
	for i := len(children) -1; i > -1; i-- {
		child := children[i]
		if snapshotPattern.MatchString(child) {
			filename = child
			tmp, _ := strconv.ParseInt(snapshotPattern.FindString(child), 10, 64)
			t = time.Unix(0, tmp)
			ok = true
			return
		}
	}
	return
}
func (self *Shard) getStreams(after time.Time) []string {
        directory, err := os.Open(self.dir)
        if err != nil {
                panic(fmt.Errorf("While trying to find streams for %v: %v", self, err))
        }
        children, err := directory.Readdirnames(-1)
        if err != nil {
		panic(fmt.Errorf("While trying to find streams for %v: %v", self, err))
        }
        sort.Sort(logNames(children))
	var rval []string
	for _, child := range children {
		if streamPattern.MatchString(child) {
			tmp, _ := strconv.ParseInt(streamPattern.FindString(child), 10, 64)
			t := time.Unix(0, tmp)
			if t.After(after) {
				rval = append(rval, child)
			}
		}
	}
	return rval
}
func (self *Shard) load() {
	self.restoring = true
	latestSnapshot, snapshotFound, snapshotTime := self.getLastSnapshot()
	if snapshotFound {
		self.loadPath(latestSnapshot)
	}
	for _, stream := range self.getStreams(snapshotTime) {
	        self.loadPath(filepath.Join(self.dir, stream))
        }
	self.restoring = false
}
func (self *Shard) newStreamFile() *os.File {
	filename := filepath.Join(self.dir, fmt.Sprintf(streamFormat, time.Now().UnixNano()))
	logfile, err := os.Create(filename)
	if err != nil {
		panic(fmt.Errorf("While trying to create %v: %v", filename, err))
	}
	return logfile
}
func (self *Shard) snapshot() {
	if atomic.CompareAndSwapInt32(&self.snapshotting, 0, 1) {
		fmt.Println("implement snapshot!")
		defer atomic.StoreInt32(&self.snapshotting, 0)
	}
}
func (self *Shard) store() {
	logfile := self.newStreamFile()
	encoder := gob.NewEncoder(logfile)
	for entry := range self.logChannel {
		if entry.operation.Command == CLEAR {
			if err := os.RemoveAll(self.dir); err != nil {
				panic(fmt.Errorf("While trying to clear %v: %v", self, err))
			}
			if err := os.MkdirAll(self.dir, 0700); err != nil {
				panic(fmt.Errorf("While trying to clear %v: %v", self, err))
			}
			self.hash = gotomic.NewHash()
			logfile = self.newStreamFile()
			encoder = gob.NewEncoder(logfile)
		} else {
			if err := encoder.Encode(entry.operation); err != nil {
				panic(fmt.Errorf("While trying to log %v: %v", entry.operation, err))
			}
			if self.tooLargeLog(logfile) {
				logfile.Close()
				go self.snapshot()
				logfile = self.newStreamFile()
			}
		}
		if entry.done != nil {
			entry.done <- true
		}
	}
	logfile.Close()
}
func (self *Shard) getMaxLogSize() int64 {
	if v, ok := self.conf[maxLogSize]; ok {
		return v.(int64)
	}
	return defaultMaxLogSize
}
func (self *Shard) tooLargeLog(logfile *os.File) bool {
	info, err := logfile.Stat()
	if err != nil {
		panic(fmt.Errorf("When trying to stat %v: %v", logfile, err))
	} 
	if info.Size() > self.getMaxLogSize() {
		return true
	}
	return false
}
func (self *Shard) log(o Operation, done chan bool) {
	if !self.restoring {
		self.logChannel <- loggedOperation{o, done}
	}
}

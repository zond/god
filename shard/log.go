
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
	snapshotFormat = "snapshot-%v.log"
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
	vi := uint64(0)
	vj := uint64(0)
	if logPattern.MatchString(self[i]) {
		vi, _ = strconv.ParseUint(logPattern.FindStringSubmatch(self[i])[1], 10, 64)
	}
	if logPattern.MatchString(self[j]) {
		vj, _ = strconv.ParseUint(logPattern.FindStringSubmatch(self[j])[1], 10, 64)
	}
	return vi < vj
}
func (self logNames) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

func (self *Shard) loadPath(path string) error {
	file, err := os.Open(path)
	if err != nil {
		panic(fmt.Errorf("While trying to load %v: %v", path, err))
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
		panic(fmt.Errorf("While trying to load %v: %v", path, err))
	}
	return nil
}
func (self *Shard) getLogs() []string {
        directory, err := os.Open(self.dir)
        if err != nil {
                panic(fmt.Errorf("While trying to find streams for %v: %v", self, err))
        }
        children, err := directory.Readdirnames(-1)
        if err != nil {
		panic(fmt.Errorf("While trying to find streams for %v: %v", self, err))
        }
        sort.Sort(logNames(children))
	return children
}
func (self *Shard) getLastSnapshot() (filename string, ok bool, t time.Time) {
	logs := self.getLogs()
	for i := len(logs) -1; i > -1; i-- {
		log := logs[i]
		if snapshotPattern.MatchString(log) {
			filename = log
			tmp, _ := strconv.ParseInt(snapshotPattern.FindStringSubmatch(log)[1], 10, 64)
			t = time.Unix(0, tmp)
			ok = true
			return
		}
	}
	return
}
func (self *Shard) getStreamsAfter(after time.Time) []string {
	var rval []string
	for _, child := range self.getLogs() {
		if streamPattern.MatchString(child) {
			tmp, _ := strconv.ParseInt(streamPattern.FindStringSubmatch(child)[1], 10, 64)
			t := time.Unix(0, tmp)
			if after.IsZero() || !after.Before(t) {
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
		self.loadPath(filepath.Join(self.dir, latestSnapshot))
	}
	for _, stream := range self.getStreamsAfter(snapshotTime) {
	        self.loadPath(filepath.Join(self.dir, stream))
        }
	self.restoring = false
}
func (self *Shard) newStreamFile(t time.Time) *os.File {
	filename := filepath.Join(self.dir, fmt.Sprintf(streamFormat, t.UnixNano()))
	logfile, err := os.Create(filename)
	if err != nil {
		panic(fmt.Errorf("While trying to create %v: %v", filename, err))
	}
	return logfile
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
		self.hash.Each(func(k gotomic.Hashable, v gotomic.Thing) {
			if err = encoder.Encode(&Operation{PUT, []string{string(k.(gotomic.StringKey)), v.(string)}}); err != nil {
				panic(fmt.Errorf("While trying to create %v: %v", tmpfilename, err))
			}
		}) 
		snapshot.Close()
		if err = os.Rename(tmpfilename, filename); err != nil {
			panic(fmt.Errorf("While trying to rename %v to %v: %v", tmpfilename, filename, err))
		}
		for _, log := range self.getLogs() {
			tmp, _ := strconv.ParseInt(logPattern.FindStringSubmatch(log)[1], 10, 64)
			logtime := time.Unix(0, tmp)
			if logtime.Before(t) {
				os.Remove(filepath.Join(self.dir, log))
			}
		}
	}
}
func (self *Shard) store() {
	logfile := self.newStreamFile(time.Now())
	encoder := gob.NewEncoder(logfile)
	for entry := range self.logChannel {
		if entry.operation.Command == CLEAR {
			logfile.Close()
			if err := os.RemoveAll(self.dir); err != nil {
				panic(fmt.Errorf("While trying to clear %v: %v", self, err))
			}
			if err := os.MkdirAll(self.dir, 0700); err != nil {
				panic(fmt.Errorf("While trying to clear %v: %v", self, err))
			}
			self.hash = gotomic.NewHash()
			logfile = self.newStreamFile(time.Now())
			encoder = gob.NewEncoder(logfile)
		} else {
			if err := encoder.Encode(entry.operation); err != nil {
				panic(fmt.Errorf("While trying to log %v: %v", entry.operation, err))
			}
			if self.tooLargeLog(logfile) {
				logfile.Close()
				t := time.Now()
				go self.snapshot(t)
				logfile = self.newStreamFile(t)
				encoder = gob.NewEncoder(logfile)
			}
		}
		if entry.done != nil {
			entry.done <- true
		}
	}
	logfile.Close()
}
func (self *Shard) SetMaxLogSize(m int64) {
	self.conf[maxLogSize] = m
}
func (self *Shard) GetMaxLogSize() int64 {
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
	if info.Size() > self.GetMaxLogSize() {
		return true
	}
	return false
}
func (self *Shard) log(o Operation, done chan bool) {
	if !self.restoring {
		self.logChannel <- loggedOperation{o, done}
	}
}

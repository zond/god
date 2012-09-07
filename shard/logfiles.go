
package shard

import (
	"regexp"
	"time"
	"os"
	"fmt"
	"sort"
	"strconv"
	"path/filepath"
)

const (
	snapshotFormat = "snapshot-%v.log"
	streamFormat = "stream-%v.log"
	followFormat = "follow-%v.log"
	maxLogSize = "maxLogSize"
	defaultMaxLogSize = 1024 * 1024 * 128
)

var streamPattern = regexp.MustCompile("^stream-(\\d+)\\.log$")
var followPattern = regexp.MustCompile("^follow-(\\d+)\\.log$")
var snapshotPattern = regexp.MustCompile("^snapshot-(\\d+)\\.log$")
var logPattern = regexp.MustCompile("^\\w+-(\\d+)\\.log$")

func (self *Shard) getLogs(pattern *regexp.Regexp) []string {
        directory, err := os.Open(self.dir)
        if err != nil {
                panic(fmt.Errorf("While trying to find streams for %v: %v", self, err))
        }
        children, err := directory.Readdirnames(-1)
        if err != nil {
		panic(fmt.Errorf("While trying to find streams for %v: %v", self, err))
        }
	var rval []string
	for _, child := range children {
		if pattern.MatchString(child) {
			rval = append(rval, child)
		}
	}
        sort.Sort(logNames(rval))
	return rval
}
func (self *Shard) getLastSnapshot() (filename string, t time.Time, ok bool) {
	logs := self.getLogs(snapshotPattern)
	if len(logs) > 0 {
		filename = logs[len(logs) - 1]
		tmp, _ := strconv.ParseInt(snapshotPattern.FindStringSubmatch(filename)[1], 10, 64)
		t = time.Unix(0, tmp)
		ok = true
		return
	}
	return
}
func (self *Shard) getStreamsAfter(after time.Time) []string {
	var rval []string
	for _, child := range self.getLogs(streamPattern) {
		tmp, _ := strconv.ParseInt(streamPattern.FindStringSubmatch(child)[1], 10, 64)
		t := time.Unix(0, tmp)
		if after.IsZero() || !after.Before(t) {
			rval = append(rval, child)
		}
	}
	return rval
}
func (self *Shard) newLogFile(t time.Time, format string) *os.File {
	filename := filepath.Join(self.dir, fmt.Sprintf(format, t.UnixNano()))
	logfile, err := os.Create(filename)
	if err != nil {
		panic(fmt.Errorf("While trying to create %v: %v", filename, err))
	}
	return logfile
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
func (self *Shard) getOldestFollow() (path string, t time.Time) {
	logs := self.getLogs(followPattern)
	path = logs[len(logs) - 1]
	tmp, _ := strconv.ParseInt(followPattern.FindStringSubmatch(path)[1], 10, 64)
	t = time.Unix(0, tmp)
	return
}
func (self *Shard) getNextFollow(after time.Time) (path string, t time.Time, ok bool) {
	for _, log := range self.getLogs(followPattern) {
		tmp, _ := strconv.ParseInt(followPattern.FindStringSubmatch(log)[1], 10, 64)
		this_t := time.Unix(0, tmp)
		if this_t.After(after) {
			path = log
			ok = true
			t = this_t
			return
		}
	}
	return
}

package persistence

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"sync/atomic"
	"time"
)

var logfileReg = regexp.MustCompile("^(\\d+)\\.(snap|log)$")

const (
	stopped = iota
	recording
	playing
)

type Op struct {
	Key        []byte
	SubKey     []byte
	Value      []byte
	Version    int64
	SubVersion int64
	Put        bool
}

type logfile struct {
	timestamp time.Time
	filename  string
	file      *os.File
	encoder   *gob.Encoder
	decoder   *gob.Decoder
}

func createLogfile(dir, suffix string) (rval *logfile) {
	rval = &logfile{}
	rval.timestamp = time.Now()
	rval.filename = filepath.Join(dir, fmt.Sprintf("%v%v", rval.timestamp.UnixNano(), suffix))
	return
}

func parseLogfile(file string) (rval *logfile, err error) {
	match := logfileReg.FindStringSubmatch(filepath.Base(file))
	if match == nil {
		err = fmt.Errorf("%v does not match %v", file, logfileReg)
		return
	}
	nanos, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil {
		return
	}
	rval = &logfile{}
	rval.timestamp = time.Unix(0, nanos)
	rval.filename = file
	return
}

func (self *logfile) read() *logfile {
	var err error
	self.file, err = os.Open(self.filename)
	if err != nil {
		panic(err)
	}
	self.decoder = gob.NewDecoder(self.file)
	return self
}

func (self *logfile) write() *logfile {
	var err error
	self.file, err = os.Create(self.filename)
	if err != nil {
		panic(err)
	}
	self.encoder = gob.NewEncoder(self.file)
	return self
}

func (self *logfile) close() {
	self.file.Close()
}

type logfiles []*logfile

func (self logfiles) Len() int {
	return len(self)
}
func (self logfiles) Less(i, j int) bool {
	return self[i].timestamp.Before(self[j].timestamp)
}
func (self logfiles) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

type Operate func(o Op)

type Snapshot func(p *Persistence)

type Persistence struct {
	ops      chan Op
	stops    chan chan bool
	dir      string
	state    int32
	snapping int32
	maxSize  int64
	snapshot Snapshot
	suffix   string
}

func NewPersistence(dir string) *Persistence {
	if err := os.MkdirAll(dir, os.ModePerm); err != nil {
		panic(err)
	}
	return &Persistence{
		ops:    make(chan Op),
		stops:  make(chan chan bool),
		dir:    dir,
		suffix: ".log",
	}
}

func (self *Persistence) hasState(s int32) bool {
	return atomic.LoadInt32(&self.state) == s
}
func (self *Persistence) changeState(old, neu int32) bool {
	return atomic.CompareAndSwapInt32(&self.state, old, neu)
}

func (self *Persistence) setSuffix(s string) *Persistence {
	self.suffix = s
	return self
}

func (self *Persistence) Limit(maxSize int64, snapshot Snapshot) *Persistence {
	self.maxSize, self.snapshot = maxSize, snapshot
	return self
}

func (self *Persistence) play(log *logfile, operate Operate) {
	log.read()
	var op Op
	var err error
	err = log.decoder.Decode(&op)
	for err == nil {
		operate(op)
		err = log.decoder.Decode(&op)
	}
	if err != io.EOF {
		panic(err)
	}
}

func (self *Persistence) latest() (latestSnapshot *logfile, logs logfiles) {
	dir, err := os.Open(self.dir)
	if err != nil {
		panic(err)
	}
	files, err := dir.Readdirnames(0)
	if err != nil {
		panic(err)
	}
	var match []string
	for _, file := range files {
		if match = logfileReg.FindStringSubmatch(file); match != nil && match[2] == "snap" {
			snapshot, err := parseLogfile(filepath.Join(self.dir, file))
			if err != nil {
				panic(err)
			}
			if latestSnapshot == nil || latestSnapshot.timestamp.After(snapshot.timestamp) {
				latestSnapshot = snapshot
			}
		}
	}
	for _, file := range files {
		if match = logfileReg.FindStringSubmatch(file); match != nil && match[2] == "log" {
			logf, err := parseLogfile(filepath.Join(self.dir, file))
			if err != nil {
				panic(err)
			}
			if latestSnapshot == nil || latestSnapshot.timestamp.Before(logf.timestamp) {
				logs = append(logs, logf)
			}
		}
	}
	sort.Sort(logs)
	return
}

func (self *Persistence) Play(operate Operate) {
	if self.changeState(stopped, playing) {
		defer self.changeState(playing, stopped)
		snapshot, logs := self.latest()
		if snapshot != nil {
			self.play(snapshot, operate)
		}
		for _, logf := range logs {
			self.play(logf, operate)
		}
	}
}

func (self *Persistence) Stop() *Persistence {
	if self.hasState(recording) {
		stop := make(chan bool)
		self.stops <- stop
		<-stop
	} else {
		panic(fmt.Errorf("%v is not in state recording", self))
	}
	return self
}

func (self *Persistence) clearOlderThan(t time.Time) {
	dir, err := os.Open(self.dir)
	if err != nil {
		panic(err)
	}
	files, err := dir.Readdirnames(0)
	if err != nil {
		panic(err)
	}
	for _, filename := range files {
		if logf, err := parseLogfile(filepath.Join(self.dir, filename)); err == nil {
			if logf.timestamp.Before(t) {
				if err = os.Remove(filepath.Join(self.dir, filename)); err != nil {
					log.Printf("failed removing %v: %v", filename, err)
				}
			}
		}
	}
}

func (self *Persistence) snapshotAndDelete(oldrec *logfile, p chan *logfile, snapping *int32) {
	defer atomic.StoreInt32(snapping, 0)
	snapshotter := NewPersistence(self.dir).setSuffix(".unfinished")
	newlogfile := <-snapshotter.Record()
	p <- newlogfile
	self.snapshot(snapshotter)
	snapshotter.Stop()
	if err := os.Rename(newlogfile.filename, filepath.Join(self.dir, fmt.Sprintf("%v%v", newlogfile.timestamp.UnixNano(), ".snap"))); err != nil {
		panic(err)
	}
	self.clearOlderThan(newlogfile.timestamp)
}

func (self *Persistence) swap(fi *os.FileInfo, err *error, rec *logfile) *logfile {
	if atomic.LoadInt32(&self.snapping) == 0 {
		if *fi, *err = os.Stat(rec.filename); *err != nil {
			panic(*err)
		}
		if (*fi).Size() > self.maxSize {
			rec.close()
			started := make(chan *logfile)
			atomic.StoreInt32(&self.snapping, 1)
			go self.snapshotAndDelete(rec, started, &self.snapping)
			<-started
			rec = createLogfile(self.dir, self.suffix)
			rec.write()
		}
	}
	return rec
}

func (self *Persistence) Record() (rval chan *logfile) {
	if !self.changeState(stopped, recording) {
		panic(fmt.Errorf("%v unable to change state from stopped to recording", self))
	}
	rval = make(chan *logfile, 1)
	go self.record(rval)
	return
}

func (self *Persistence) record(p chan *logfile) {
	var err error
	var op Op
	var fi os.FileInfo
	var stop chan bool

	rec := createLogfile(self.dir, self.suffix)
	rec.write()
	p <- rec
	defer rec.close()

	for {
		if self.maxSize != 0 {
			rec = self.swap(&fi, &err, rec)
		}

		select {
		case op = <-self.ops:
			if err = rec.encoder.Encode(op); err != nil {
				panic(err)
			}
		case stop = <-self.stops:
			if !self.changeState(recording, stopped) {
				panic(fmt.Errorf("%v unable to change state from recording to stopped", self))
			}
			stop <- true
			return
		}
		select {
		case stop = <-self.stops:
			if !self.changeState(recording, stopped) {
				panic(fmt.Errorf("%v unable to change state from recording to stopped", self))
			}
			stop <- true
			return
		default:
		}
	}
}

func (self *Persistence) Dump(o Op) {
	if !self.hasState(recording) {
		panic(fmt.Errorf("%v is not recording", self))
	}
	self.ops <- o
}

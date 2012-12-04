package persistence

import (
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
)

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

type Operate func(o Op)

type NextFile func() string

type Snapshot func()

type Persistence struct {
	ops      chan Op
	stops    chan bool
	file     string
	state    int32
	maxSize  int64
	nextFile NextFile
	snapshot Snapshot
}

func NewPersistence(file string) *Persistence {
	return &Persistence{
		ops:   make(chan Op),
		stops: make(chan bool),
		file:  file,
	}
}

func (self *Persistence) Snapshot(maxSize int64, snapshot Snapshot, nextFile NextFile) *Persistence {
	self.maxSize, self.snapshot, self.nextFile = maxSize, snapshot, nextFile
	return self
}

func (self *Persistence) hasState(s int32) bool {
	return atomic.LoadInt32(&self.state) == s
}
func (self *Persistence) changeState(old, neu int32) bool {
	return atomic.CompareAndSwapInt32(&self.state, old, neu)
}

func (self *Persistence) Record() *Persistence {
	if self.changeState(stopped, recording) {
		go self.record()
	}
	return self
}

func (self *Persistence) Play(operate Operate) {
	if self.changeState(stopped, playing) {
		defer self.changeState(playing, stopped)
		in, err := os.Open(self.file)
		if err != nil {
			panic(err)
		}
		defer in.Close()
		decoder := gob.NewDecoder(in)
		var op Op
		err = decoder.Decode(&op)
		for err == nil {
			operate(op)
			err = decoder.Decode(&op)
		}
		if err != io.EOF {
			panic(err)
		}
	}
}

func (self *Persistence) Stop() *Persistence {
	self.stops <- true
	return self
}

func (self *Persistence) snapshotAndDelete(oldfile string) {
	self.snapshot()
	if err := os.Remove(oldfile); err != nil {
		log.Println(err)
	}
}

func (self *Persistence) swap(fi *os.FileInfo, err *error, out *os.File, encoder *gob.Encoder) (*os.File, *gob.Encoder) {
	if *fi, *err = os.Stat(self.file); *err != nil {
		panic(*err)
	}
	if (*fi).Size() > self.maxSize {
		out.Close()
		oldfile := self.file
		self.file = self.nextFile()
		if out, *err = os.Create(self.file); *err != nil {
			panic(*err)
		}
		encoder = gob.NewEncoder(out)
		go self.snapshotAndDelete(oldfile)
	}
	return out, encoder
}

func (self *Persistence) record() {
	var out *os.File
	var err error
	var op Op
	var fi os.FileInfo

	if out, err = os.Create(self.file); err != nil {
		panic(err)
	}
	defer out.Close()

	encoder := gob.NewEncoder(out)
	for {
		if self.maxSize != 0 {
			out, encoder = self.swap(&fi, &err, out, encoder)
		}

		select {
		case op = <-self.ops:
			if err = encoder.Encode(op); err != nil {
				panic(err)
			}
		case _ = <-self.stops:
			self.changeState(recording, stopped)
			break
		}
		select {
		case _ = <-self.stops:
			self.changeState(recording, stopped)
			break
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

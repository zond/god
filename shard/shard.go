
package shard

import (
	"regexp"
	"fmt"
	locks "../locks"
	"time"
	"io"
	"strconv"
	"os"
	"runtime"
	"log"
	"encoding/gob"
	"sort"
	"path/filepath"
)

const (
	snapshot = "snapshot.log"
	streamFormat = "stream-%v.log"
	arity_error = "Illegal number of parameters. Wanted %v but got %v."
)

var streamPattern = regexp.MustCompile("^stream-(\\d+)\\.log$")

type Command int
const (
	GET = Command(iota)
	PUT 
	DELETE
	KEYS
	CLEAR
)
func (self Command) String() string {
	switch self {
	case GET:
		return "GET"
	case PUT:
		return "PUT"
	case DELETE:
		return "DELETE"
	case KEYS:
		return "KEYS"
	case CLEAR:
		return "CLEAR"
	}
	return "UNKNOWN"
}

type Result int
const (
	OK = Result(1 << iota)
	MISSING
	EXISTS
	UNKNOWN
	BAD
	ARITY
	ERROR
)
func (self Result) String() string {
	var rval []string
	if self & OK == OK {
		rval = append(rval, "OK")
	}
	if self & MISSING == MISSING {
		rval = append(rval, "MISSING")
	}
	if self & EXISTS == EXISTS {
		rval = append(rval, "EXISTS")
	}
	if self & UNKNOWN == UNKNOWN {
		rval = append(rval, "UNKNOWN")
	}
	if self & BAD == BAD {
		rval = append(rval, "BAD")
	}
	if self & ARITY == ARITY {
		rval = append(rval, "ARITY")
	}
	if self & ERROR == ERROR {
		rval = append(rval, "ARITY")
	}
	return fmt.Sprint(rval)
}

type Operation struct {
	Command Command
	Parameters []string
}

type Response struct {
	Result Result
	Parts []string
}
func (self Response) Ok() bool {
	return self.Result & OK == OK
}

type State int
const (
	RESTORING = State(iota)
	WORKING
)
func (self State) String() string {
	switch self {
	case RESTORING:
		return "RESTORING"
	case WORKING:
		return "WORKING"
	}
	return "UNKNOWN"
}

type streamNames []string
func (self streamNames) Len() int {
	return len(self)
}
func (self streamNames) Less(i, j int) bool {
	vi, _ := strconv.ParseUint(streamPattern.FindStringSubmatch(self[i])[1], 10, 64)
	vj, _ := strconv.ParseUint(streamPattern.FindStringSubmatch(self[j])[1], 10, 64)
	return vi < vj
}
func (self streamNames) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}

type Shard struct {
	hash *locks.Map
	dir string
	logChannel chan Operation
	state State
}
func NewShard(dir string) (*Shard, error) {
	rval := (&Shard{}).initialize(dir, RESTORING)
	runtime.SetFinalizer(rval, func(s *Shard) {
		close(s.logChannel)
	})
	if err := rval.load(); err != nil {
		return nil, err
	}
	go rval.store()
	rval.state = WORKING
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
func (self *Shard) initialize(dir string, state State) *Shard {
	self.hash = locks.NewMap()
	self.dir = dir
	os.MkdirAll(self.dir, 0700)
	self.logChannel = make(chan Operation)
	self.state = state
	return self
}
func (self *Shard) loadPath(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		} else {
			return err
		}
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
		log.Printf("While loading %v: %v", path, err)
	}
	return nil
}
func (self *Shard) load() error {
	if err := self.loadPath(filepath.Join(self.dir, snapshot)); err != nil {
		return err
	}
        directory, err := os.Open(self.dir)
        if err != nil {
                return err
        }
        children, err := directory.Readdirnames(-1)
        if err != nil {
                return err
        }
        sort.Sort(streamNames(children))
        for _, child := range children {
                if streamPattern.MatchString(child) {
                        if err = self.loadPath(filepath.Join(self.dir, child)); err != nil {
                                return err
                        }
                }
        }
	return nil
}
func (self *Shard) store() {
	logfile, err := os.Create(filepath.Join(self.dir, fmt.Sprintf(streamFormat, time.Now().UnixNano())))
	if err != nil {
		panic(err)
	}
	defer logfile.Close()
	encoder := gob.NewEncoder(logfile)
	for operation := range self.logChannel {
		if err = encoder.Encode(operation); err != nil {
			panic(err)
		}
	}
}
func (self *Shard) okArity(o Operation, wanted int, r *Response) bool {
	if len(o.Parameters) != wanted {
		r.Result = BAD | ARITY
		r.Parts = []string{fmt.Sprint(arity_error, wanted, len(o.Parameters))}
		return false
	}
	return true
}
func (self *Shard) get(o Operation, r *Response) {
	if !self.okArity(o, 1, r) {
		return
	}
	if v, ok := self.hash.Get(o.Parameters[0]); ok {
		r.Result = OK | EXISTS
		r.Parts = []string{v}
	} else {
		r.Result = OK | MISSING
		r.Parts = nil
	}
	return
}
func (self *Shard) log(o Operation) {
	if self.state == WORKING {
		self.logChannel <- o
	}
}
func (self *Shard) put(o Operation, r *Response) {
	if !self.okArity(o, 2, r) {
		return
	}
	self.log(o)
	self.hash.Put(o.Parameters[0], o.Parameters[1])
	r.Result = OK
	r.Parts = nil
	return
}
func (self *Shard) keys(o Operation, r *Response) {
	if !self.okArity(o, 0, r) {
		return
	}
	r.Result = OK
	r.Parts = self.hash.Keys()
}
func (self *Shard) clear(o Operation, r *Response) {
	if !self.okArity(o, 0, r) {
		return
	}
	close(self.logChannel)
	if err := os.RemoveAll(self.dir); err != nil {
		r.Result = ERROR
		r.Parts = []string{err.Error()}
		return
	} 
	self.initialize(self.dir, self.state)
	go self.store()
	r.Result = OK
	r.Parts = nil
}
func (self *Shard) del(o Operation, r *Response) {
	if !self.okArity(o, 1, r) {
		return
	}
	self.log(o)
	self.hash.Del(o.Parameters[0])
	r.Result = OK
	r.Parts = nil
}
func (self *Shard) Perform(o Operation, r *Response) {
	switch o.Command {
	case GET:
		self.get(o, r)
	case PUT:
		self.put(o, r)
	case DELETE:
		self.del(o, r)
	case KEYS:
		self.keys(o, r)
	case CLEAR:
		self.clear(o, r)
	default:
		r.Result = UNKNOWN
		r.Parts = nil
	}
}


package shard

import (
	"github.com/zond/gotomic"
	"fmt"
)

const (
	arity_error = "Illegal number of parameters. Wanted %v but got %v."
)

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

func (self *Shard) put(o Operation, r *Response) {
	if !self.okArity(o, 2, r) {
		return
	}
	self.log(o, nil)
	if old, ok := self.hash.Put(gotomic.StringKey(o.Parameters[0]), o.Parameters[1]); ok {
		r.Result = OK | EXISTS
		r.Parts = []string{old.(string)}
	} else {
		r.Result = OK | MISSING
		r.Parts = nil
	}
}
func (self *Shard) keys(o Operation, r *Response) {
	if !self.okArity(o, 0, r) {
		return
	}
	r.Result = OK
	r.Parts = nil
	self.hash.Each(func(k gotomic.Hashable, v gotomic.Thing) bool {
		r.Parts = append(r.Parts, v.(string))
		return true
	})
}
func (self *Shard) clear(o Operation, r *Response) {
	if !self.okArity(o, 0, r) {
		return
	}
	done := make(chan bool)
	self.log(o, done)
	<- done
	r.Result = OK
	r.Parts = nil
}
func (self *Shard) del(o Operation, r *Response) {
	if !self.okArity(o, 1, r) {
		return
	}
	self.log(o, nil)
	if old, ok := self.hash.Delete(gotomic.StringKey(o.Parameters[0])); ok {
		r.Result = OK | EXISTS
		r.Parts = []string{old.(string)}
	} else {
		r.Result = OK | MISSING
		r.Parts = nil
	}
}
func (self *Shard) MustPerform(o Operation) {
	response := &Response{}
	self.Perform(o, response)
	if response.Result & OK != OK {
		panic(fmt.Errorf("When trying to perform %v: %v", o, response))
	}
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
	if v, ok := self.hash.Get(gotomic.StringKey(o.Parameters[0])); ok {
		r.Result = OK | EXISTS
		r.Parts = []string{v.(string)}
	} else {
		r.Result = OK | MISSING
		r.Parts = nil
	}
}

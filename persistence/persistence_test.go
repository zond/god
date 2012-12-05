package persistence

import (
	"fmt"
	"os"
	"reflect"
	"testing"
)

func operator(ary *[]Op) Operate {
	return func(o Op) {
		*ary = append(*ary, o)
	}
}

func TestRecordPlay(t *testing.T) {
	os.RemoveAll("test1")
	p := NewPersistence("test1")
	p.Record()
	op := Op{
		Key:     []byte("a"),
		Value:   []byte("1"),
		Version: 1,
	}
	p.Dump(op)
	p.Stop()
	var ary []Op
	p.Play(operator(&ary))
	if !reflect.DeepEqual(ary, []Op{op}) {
		t.Errorf("%+v should be %+v", ary, []Op{op})
	}
}

type testmap struct {
	m map[string]string
	p *Persistence
}

func newTestmap() (rval testmap) {
	rval.m = make(map[string]string)
	os.RemoveAll("test3")
	rval.p = NewPersistence("test3")
	rval.p.Limit(1024, rval.snapshotter())
	rval.p.Record()
	return
}
func (self testmap) put(k, v string) {
	self.p.Dump(Op{
		Key:   []byte(k),
		Value: []byte(v),
	})
	self.m[k] = v
}
func (self testmap) operator() Operate {
	return func(o Op) {
		if o.Put {
			self.m[string(o.Key)] = string(o.Value)
		} else {
			delete(self.m, string(o.Key))
		}
	}
}
func (self testmap) snapshotter() Snapshot {
	return func(p *Persistence) {
		op := Op{}
		for k, v := range self.m {
			op.Key = []byte(k)
			op.Value = []byte(v)
			p.Dump(op)
		}
	}
}

func TestSwap(t *testing.T) {
	tm := newTestmap()
	for i := 0; i < 1000; i++ {
		tm.put(fmt.Sprint(i), fmt.Sprint(i))
	}
}

func BenchmarkRecord(b *testing.B) {
	b.StopTimer()
	os.RemoveAll("test2")
	p := NewPersistence("test2")
	p.Record()
	op := Op{
		Key:     []byte("a"),
		Value:   []byte("1"),
		Version: 1,
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		p.Dump(op)
	}
}

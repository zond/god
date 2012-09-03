
package shard

import (
	"os"
	"path/filepath"
	"testing"
	"reflect"
	"fmt"
)

func testPerform(t *testing.T, s *Shard, o Operation, expected Response) {
	r := &Response{}
	s.Perform(o, r)
	if r.Result != expected.Result {
		t.Errorf("s.Perform(%v, ...) expected %v but got %v", o, expected, r)
	}
	if !reflect.DeepEqual(r.Parts, expected.Parts) {
		t.Errorf("s.Perform(%v, ...) expected %v but got %v", o, expected, r)
	}
}

func BenchmarkShardPut(b *testing.B) {
	b.StopTimer()
	s, err := NewEmptyShard("test2")
	if err != nil {
		b.Errorf("while trying to create empty shard: %v", err)
	}
	r := &Response{}
	o := Operation{Command: PUT, Parameters: []string{"", "v"}}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		o.Parameters[0] = fmt.Sprint("k", i)
		s.Perform(o, r)
		if r.Result & OK != OK {
			b.Fatalf("%v produced %v", o, r)
		}
	}
}

func TestShardSnapshot(t *testing.T) {
	s, err := NewEmptyShard("test2")
	if err != nil {
		t.Errorf("while trying to create empty shard: %v", err)
	}
	s.SetMaxLogSize(0)
	testPerform(t, s, Operation{GET, []string{"k"}}, Response{OK | MISSING, nil})
	testPerform(t, s, Operation{PUT, []string{"k", "v"}}, Response{OK | MISSING, nil})
	testPerform(t, s, Operation{PUT, []string{"k", "v"}}, Response{OK | EXISTS, []string{"v"}})
	testPerform(t, s, Operation{GET, []string{"k"}}, Response{OK | EXISTS, []string{"v"}})
	s, err = NewShard("test2")
	if err != nil {
		t.Errorf("while trying to load shard: %v", err)
	}
	testPerform(t, s, Operation{PUT, []string{"k", "v"}}, Response{OK | EXISTS, []string{"v"}})
	testPerform(t, s, Operation{GET, []string{"k"}}, Response{OK | EXISTS, []string{"v"}})
	for _, log := range s.getLogs() {
		if !snapshotPattern.MatchString(log) {
			os.Remove(filepath.Join(s.dir, log))
		}
	}
	s, err = NewShard("test2")
	if err != nil {
		t.Errorf("while trying to load shard: %v", err)
	}
	testPerform(t, s, Operation{PUT, []string{"k", "v"}}, Response{OK | EXISTS, []string{"v"}})
	testPerform(t, s, Operation{GET, []string{"k"}}, Response{OK | EXISTS, []string{"v"}})
}

func TestShardBasicOps(t *testing.T) {
	s, err := NewEmptyShard("test1")
	if err != nil {
		t.Errorf("while trying to create empty shard: %v", err)
	}
	testPerform(t, s, Operation{GET, []string{"k"}}, Response{OK | MISSING, nil})
	testPerform(t, s, Operation{PUT, []string{"k", "v"}}, Response{OK | MISSING, nil})
	testPerform(t, s, Operation{PUT, []string{"k", "v"}}, Response{OK | EXISTS, []string{"v"}})
	testPerform(t, s, Operation{GET, []string{"k"}}, Response{OK | EXISTS, []string{"v"}})
	s, err = NewShard("test1")
	if err != nil {
		t.Errorf("while trying to load shard: %v", err)
	}
	testPerform(t, s, Operation{GET, []string{"k"}}, Response{OK | EXISTS, []string{"v"}})
	testPerform(t, s, Operation{CLEAR, []string{}}, Response{OK, nil})
	testPerform(t, s, Operation{GET, []string{"k"}}, Response{OK | MISSING, nil})
	testPerform(t, s, Operation{PUT, []string{"k", "v"}}, Response{OK | MISSING, nil})
	testPerform(t, s, Operation{GET, []string{"k"}}, Response{OK | EXISTS, []string{"v"}})
	testPerform(t, s, Operation{DELETE, []string{"k"}}, Response{OK | EXISTS, []string{"v"}})
	testPerform(t, s, Operation{DELETE, []string{"k"}}, Response{OK | MISSING, nil})
	testPerform(t, s, Operation{GET, []string{"k"}}, Response{OK | MISSING, nil})
}

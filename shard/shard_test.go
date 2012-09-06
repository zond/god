
package shard

import (
	"os"
	"path/filepath"
	"testing"
	"reflect"
	"fmt"
	"time"
)

func testPerformWithin(t *testing.T, s *Shard, o Operation, expected Response, within time.Duration) {
	deadline := time.Now().Add(within)
	for {
		if err := doesPerform(s, o, expected); err != nil && time.Now().After(deadline) {
			t.Error(err.Error())
			return
		} else {
			return
		}
		time.Sleep(time.Second / 10)
	}
}

func doesPerform(s *Shard, o Operation, expected Response) error {
	r := &Response{}
	s.Perform(o, r)
	if r.Result != expected.Result {
		return fmt.Errorf("s.Perform(%v, ...) expected %v but got %v", o, expected, r)
	}
	if !reflect.DeepEqual(r.Parts, expected.Parts) {
		return fmt.Errorf("s.Perform(%v, ...) expected %v but got %v", o, expected, r)
	}
	return nil
}

func testPerform(t *testing.T, s *Shard, o Operation, expected Response) {
	if err := doesPerform(s, o, expected); err != nil {
		t.Error(err.Error())
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

func TestShardSlaving(t *testing.T) {
	s, err := NewEmptyShard("test3")
	if err != nil {
		t.Errorf("while trying to create empty shard: %v", err)
	}
	s2, err := NewEmptyShard("test4")
	if err != nil {
		t.Errorf("while trying to create empty shard: %v", err)
	}
	s2.SetMaxLogSize(0)
	testPerform(t, s2, Operation{GET, []string{"k"}}, Response{OK | MISSING, nil})
	testPerform(t, s2, Operation{PUT, []string{"k", "v"}}, Response{OK | MISSING, nil})
	testPerform(t, s2, Operation{GET, []string{"k"}}, Response{OK | EXISTS, []string{"v"}})
	snapshot := make(chan Operation)
	stream := make(chan Operation)
	s.addSlave(snapshot, stream)
	s2.setMaster(snapshot, stream)
	testPerform(t, s2, Operation{GET, []string{"k"}}, Response{OK | MISSING, nil})
	testPerform(t, s, Operation{PUT, []string{"k", "v"}}, Response{OK | MISSING, nil})
	testPerform(t, s, Operation{PUT, []string{"k2", "v"}}, Response{OK | MISSING, nil})
	testPerform(t, s, Operation{PUT, []string{"k3", "v"}}, Response{OK | MISSING, nil})
	testPerform(t, s, Operation{PUT, []string{"k4", "v"}}, Response{OK | MISSING, nil})
	testPerformWithin(t, s2, Operation{GET, []string{"k"}}, Response{OK | EXISTS, []string{"v"}}, time.Second)
	testPerformWithin(t, s2, Operation{GET, []string{"k"}}, Response{OK | EXISTS, []string{"v2"}}, time.Second)
	testPerformWithin(t, s2, Operation{GET, []string{"k"}}, Response{OK | EXISTS, []string{"v3"}}, time.Second)
	testPerformWithin(t, s2, Operation{GET, []string{"k"}}, Response{OK | EXISTS, []string{"v4"}}, time.Second)
}

func TestShardSnapshot(t *testing.T) {
	s, err := NewEmptyShard("test5")
	if err != nil {
		t.Errorf("while trying to create empty shard: %v", err)
	}
	s.SetMaxLogSize(0)
	testPerform(t, s, Operation{GET, []string{"k"}}, Response{OK | MISSING, nil})
	testPerform(t, s, Operation{PUT, []string{"k", "v"}}, Response{OK | MISSING, nil})
	testPerform(t, s, Operation{PUT, []string{"k", "v"}}, Response{OK | EXISTS, []string{"v"}})
	testPerform(t, s, Operation{GET, []string{"k"}}, Response{OK | EXISTS, []string{"v"}})
	time.Sleep(time.Second / 20)
	s2, err := NewShard("test5")
	if err != nil {
		t.Errorf("while trying to load shard: %v", err)
	}
	testPerform(t, s2, Operation{PUT, []string{"k", "v"}}, Response{OK | EXISTS, []string{"v"}})
	testPerform(t, s2, Operation{GET, []string{"k"}}, Response{OK | EXISTS, []string{"v"}})
	for _, log := range s2.getLogs() {
		if !snapshotPattern.MatchString(log) {
			os.Remove(filepath.Join(s2.dir, log))
		}
	}
	s2.Close()
	s2, err = NewShard("test5")
	if err != nil {
		t.Errorf("while trying to load shard: %v", err)
	}
	testPerform(t, s2, Operation{PUT, []string{"k", "v"}}, Response{OK | EXISTS, []string{"v"}})
	testPerform(t, s2, Operation{GET, []string{"k"}}, Response{OK | EXISTS, []string{"v"}})
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

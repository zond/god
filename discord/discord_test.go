package discord

import (
	"reflect"
	"testing"
)

func assertIndices(t *testing.T, r *Ring, pos byte, before, at, after int) {
	a, b, c := r.indices([]byte{pos})
	if a != before {
		t.Errorf("%v.indices([]byte{%v}) should be %v,%v,%v but was %v,%v,%v", r, pos, before, at, after, a, b, c)
	}
	if b != at {
		t.Errorf("%v.indices([]byte{%v}) should be %v,%v,%v but was %v,%v,%v", r, pos, before, at, after, a, b, c)
	}
	if c != after {
		t.Errorf("%v.indices([]byte{%v}) should be %v,%v,%v but was %v,%v,%v", r, pos, before, at, after, a, b, c)
	}
}

func TestRingIndices(t *testing.T) {
	r, _ := buildRing()
	assertIndices(t, r, 0, 6, 0, 1)
	assertIndices(t, r, 1, 0, 1, 2)
	assertIndices(t, r, 2, 1, 2, 3)
	assertIndices(t, r, 3, 2, 3, 4)
	assertIndices(t, r, 4, 3, 4, 5)
	assertIndices(t, r, 5, 4, -1, 5)
	assertIndices(t, r, 6, 4, 5, 6)
	assertIndices(t, r, 7, 5, 6, 0)
}

func buildRing() (*Ring, []Remote) {
	r := &Ring{}
	var cmp []Remote
	r.add(Remote{[]byte{0}, "a"})
	cmp = append(cmp, Remote{[]byte{0}, "a"})
	r.add(Remote{[]byte{1}, "b"})
	cmp = append(cmp, Remote{[]byte{1}, "b"})
	r.add(Remote{[]byte{2}, "c"})
	cmp = append(cmp, Remote{[]byte{2}, "c"})
	r.add(Remote{[]byte{3}, "d"})
	cmp = append(cmp, Remote{[]byte{3}, "d"})
	r.add(Remote{[]byte{4}, "e"})
	cmp = append(cmp, Remote{[]byte{4}, "e"})
	r.add(Remote{[]byte{6}, "f"})
	cmp = append(cmp, Remote{[]byte{6}, "f"})
	r.add(Remote{[]byte{7}, "g"})
	cmp = append(cmp, Remote{[]byte{7}, "g"})
	return r, cmp
}

func TestRingClean(t *testing.T) {
	r, cmp := buildRing()
	r.clean([]byte{0}, []byte{2})
	cmp = append(cmp[:1], cmp[2:]...)
	if !reflect.DeepEqual(r.Nodes, cmp) {
		t.Error(r.Nodes, "should ==", cmp)
	}
	r, cmp = buildRing()
	r.clean([]byte{0}, []byte{1})
	if !reflect.DeepEqual(r.Nodes, cmp) {
		t.Error(r.Nodes, "should ==", cmp)
	}
	r, cmp = buildRing()
	r.clean([]byte{4}, []byte{6})
	if !reflect.DeepEqual(r.Nodes, cmp) {
		t.Error(r.Nodes, "should ==", cmp)
	}
	r, cmp = buildRing()
	r.clean([]byte{7}, []byte{0})
	if !reflect.DeepEqual(r.Nodes, cmp) {
		t.Error(r.Nodes, "should ==", cmp)
	}
	r, cmp = buildRing()
	r.clean([]byte{7}, []byte{1})
	cmp = cmp[1:]
	if !reflect.DeepEqual(r.Nodes, cmp) {
		t.Error(r.Nodes, "should ==", cmp)
	}
	r, cmp = buildRing()
	r.clean([]byte{6}, []byte{0})
	cmp = cmp[:6]
	if !reflect.DeepEqual(r.Nodes, cmp) {
		t.Error(r.Nodes, "should ==", cmp)
	}
}

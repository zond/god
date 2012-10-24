package common

import (
	"reflect"
	"testing"
)

func assertIndices(t *testing.T, r *ring, pos, before, at, after byte) {
	a, b, c := r.Remotes([]byte{pos})
	if (a == nil && before != 255) || (a != nil && a.Pos[0] != before) {
		t.Errorf("%v.indices([]byte{%v}) should be %v,%v,%v but was %v,%v,%v", r, pos, before, at, after, a, b, c)
	}
	if (b == nil && at != 255) || (b != nil && b.Pos[0] != at) {
		t.Errorf("%v.indices([]byte{%v}) should be %v,%v,%v but was %v,%v,%v", r, pos, before, at, after, a, b, c)
	}
	if (c == nil && after != 255) || (c != nil && c.Pos[0] != after) {
		t.Errorf("%v.indices([]byte{%v}) should be %v,%v,%v but was %v,%v,%v", r, pos, before, at, after, a, b, c)
	}
}

func TestRingIndices(t *testing.T) {
	r, _ := buildRing()
	assertIndices(t, r, 0, 7, 0, 1)
	assertIndices(t, r, 1, 0, 1, 2)
	assertIndices(t, r, 2, 1, 2, 3)
	assertIndices(t, r, 3, 2, 3, 4)
	assertIndices(t, r, 4, 3, 4, 6)
	assertIndices(t, r, 5, 4, 255, 6)
	assertIndices(t, r, 6, 4, 6, 7)
	assertIndices(t, r, 7, 6, 7, 0)
}

func buildRing() (*ring, []Remote) {
	r := NewRing()
	var cmp []Remote
	r.Add(Remote{[]byte{0}, "a"})
	cmp = append(cmp, Remote{[]byte{0}, "a"})
	r.Add(Remote{[]byte{1}, "b"})
	cmp = append(cmp, Remote{[]byte{1}, "b"})
	r.Add(Remote{[]byte{2}, "c"})
	cmp = append(cmp, Remote{[]byte{2}, "c"})
	r.Add(Remote{[]byte{3}, "d"})
	cmp = append(cmp, Remote{[]byte{3}, "d"})
	r.Add(Remote{[]byte{4}, "e"})
	cmp = append(cmp, Remote{[]byte{4}, "e"})
	r.Add(Remote{[]byte{6}, "f"})
	cmp = append(cmp, Remote{[]byte{6}, "f"})
	r.Add(Remote{[]byte{7}, "g"})
	cmp = append(cmp, Remote{[]byte{7}, "g"})
	return r, cmp
}

func TestRingClean(t *testing.T) {
	r, cmp := buildRing()
	r.Clean([]byte{0}, []byte{2})
	cmp = append(cmp[:1], cmp[2:]...)
	if !reflect.DeepEqual(r.nodes, cmp) {
		t.Error(r.nodes, "should ==", cmp)
	}
	r, cmp = buildRing()
	r.Clean([]byte{0}, []byte{1})
	if !reflect.DeepEqual(r.nodes, cmp) {
		t.Error(r.nodes, "should ==", cmp)
	}
	r, cmp = buildRing()
	r.Clean([]byte{4}, []byte{6})
	if !reflect.DeepEqual(r.nodes, cmp) {
		t.Error(r.nodes, "should ==", cmp)
	}
	r, cmp = buildRing()
	r.Clean([]byte{7}, []byte{0})
	if !reflect.DeepEqual(r.nodes, cmp) {
		t.Error(r.nodes, "should ==", cmp)
	}
	r, cmp = buildRing()
	r.Clean([]byte{7}, []byte{1})
	cmp = cmp[1:]
	if !reflect.DeepEqual(r.nodes, cmp) {
		t.Error(r.nodes, "should ==", cmp)
	}
	r, cmp = buildRing()
	r.Clean([]byte{6}, []byte{0})
	cmp = cmp[:6]
	if !reflect.DeepEqual(r.nodes, cmp) {
		t.Error(r.nodes, "should ==", cmp)
	}
	r, cmp = buildRing()
	r.Clean([]byte{3}, []byte{3})
	cmp = cmp[3:4]
	if !reflect.DeepEqual(r.nodes, cmp) {
		t.Error(r.nodes, "should ==", cmp)
	}
}

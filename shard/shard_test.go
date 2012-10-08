package shard

import (
	"testing"
)

func assertSegment(t *testing.T, r *Ring, pos, pred, succ byte) {
	surr := r.segment([]byte{pos})
	if surr.Predecessor.Pos[0] != pred {
		t.Errorf("Wrong Predecessor for %v in %v, wanted %v but got %v", pos, r, pred, surr.Predecessor)
	}
	if surr.Successor.Pos[0] != succ {
		t.Errorf("Wrong Successor for %v in %v, wanted %v but got %v", pos, r, succ, surr.Successor)
	}
}

func assertSegmentIndices(t *testing.T, r *Ring, p1, s1, p2, s2 byte) {
	predInd, succInd := r.segmentIndices([]byte{p1}, []byte{s1})
	if r.Nodes[predInd].Pos[0] != p2 {
		t.Errorf("Wrong Predecessor for %v-%v in %v, wanted %v but got %v", p1, s1, r, p2, r.Nodes[predInd].Pos[0])
	}
	if r.Nodes[succInd].Pos[0] != s2 {
		t.Errorf("Wrong Successor for %v-%v in %v, wanted %v but got %v", p1, s1, r, s2, r.Nodes[succInd].Pos[0])
	}

}

func TestRingSegmentIndices(t *testing.T) {
	r := &Ring{}
	r.add(Remote{[]byte{0}, "a"})
	r.add(Remote{[]byte{1}, "b"})
	r.add(Remote{[]byte{2}, "c"})
	r.add(Remote{[]byte{3}, "d"})
	r.add(Remote{[]byte{4}, "e"})
	r.add(Remote{[]byte{6}, "f"})
	r.add(Remote{[]byte{7}, "g"})
	assertSegmentIndices(t, r, 0, 1, 0, 2)
	assertSegmentIndices(t, r, 1, 2, 1, 3)
	assertSegmentIndices(t, r, 2, 4, 2, 6)
	assertSegmentIndices(t, r, 6, 1, 6, 2)
	assertSegmentIndices(t, r, 4, 2, 4, 3)
}

func TestRingSegment(t *testing.T) {
	r := &Ring{}
	r.add(Remote{[]byte{0}, "a"})
	r.add(Remote{[]byte{1}, "b"})
	r.add(Remote{[]byte{2}, "c"})
	r.add(Remote{[]byte{3}, "d"})
	r.add(Remote{[]byte{4}, "e"})
	r.add(Remote{[]byte{6}, "f"})
	r.add(Remote{[]byte{7}, "g"})
	assertSegment(t, r, 0, 0, 1)
	assertSegment(t, r, 1, 1, 2)
	assertSegment(t, r, 2, 2, 3)
	assertSegment(t, r, 3, 3, 4)
	assertSegment(t, r, 4, 4, 6)
	assertSegment(t, r, 5, 4, 6)
	assertSegment(t, r, 6, 6, 7)
	assertSegment(t, r, 7, 7, 0)
}

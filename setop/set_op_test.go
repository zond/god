package setop

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"testing"
)

type testSkipper struct {
	pairs []tP
	index int
}
type tP [2]string

func (self *testSkipper) Skip(min []byte, inc bool) (result *SetOpResult, err error) {
	lt := 1
	if inc {
		lt = 0
	}
	for self.index < len(self.pairs) && bytes.Compare([]byte(self.pairs[self.index][0]), min) < lt {
		self.index++
	}
	if self.index < len(self.pairs) {
		return &SetOpResult{
			Key:    []byte(self.pairs[self.index][0]),
			Values: [][]byte{[]byte(self.pairs[self.index][1])},
		}, nil
	}
	return nil, nil
}

var testSets = map[string]*testSkipper{
	"a": &testSkipper{
		pairs: []tP{
			tP{"a", "a"},
			tP{"b", "b"},
			tP{"c", "c"},
		},
	},
	"b": &testSkipper{
		pairs: []tP{
			tP{"a", "a"},
			tP{"c", "c"},
			tP{"d", "d"},
		},
	},
}

func resetSets() {
	for _, set := range testSets {
		set.index = 0
	}
}

func findTestSet(b []byte) Skipper {
	set, ok := testSets[string(b)]
	if !ok {
		panic(fmt.Errorf("couldn't find test set %s", string(b)))
	}
	return set
}

func collect(t *testing.T, expr string) []*SetOpResult {
	s, err := NewSetOpParser(expr).Parse()
	if err != nil {
		t.Fatal(err)
	}
	se := &SetExpression{
		Op: s,
	}
	var collector []*SetOpResult
	se.Each(findTestSet, func(res *SetOpResult) {
		collector = append(collector, res)
	})
	return collector
}

type testResults []*SetOpResult

func (self testResults) Len() int {
	return len(self)
}
func (self testResults) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}
func (self testResults) Less(i, j int) bool {
	return bytes.Compare(self[i].Key, self[j].Key) < 0
}

func diff(merger mergeFunc, sets ...[]tP) (result []*SetOpResult) {
	hashes := make([]map[string][]byte, len(sets))
	for index, set := range sets {
		hashes[index] = make(map[string][]byte)
		for _, pair := range set {
			hashes[index][pair[0]] = []byte(pair[1])
		}
	}
	resultMap := make(map[string][][]byte)
	for k, v := range hashes[0] {
		resultMap[k] = merger(resultMap[k], [][]byte{v})
	}
	for _, m := range hashes[1:] {
		for k, _ := range m {
			delete(resultMap, k)
		}
	}
	for k, v := range resultMap {
		result = append(result, &SetOpResult{
			Key:    []byte(k),
			Values: v,
		})
	}
	sort.Sort(testResults(result))
	return
}

func inter(merger mergeFunc, sets ...[]tP) (result []*SetOpResult) {
	hashes := make([]map[string][]byte, len(sets))
	for index, set := range sets {
		hashes[index] = make(map[string][]byte)
		for _, pair := range set {
			hashes[index][pair[0]] = []byte(pair[1])
		}
	}
	resultMap := make(map[string][][]byte)
	for _, m := range hashes {
		for k, v := range m {
			isOk := true
			for _, m2 := range hashes {
				_, ex := m2[k]
				isOk = isOk && ex
			}
			if isOk {
				resultMap[k] = merger(resultMap[k], [][]byte{v})
			}
		}
	}
	for k, v := range resultMap {
		result = append(result, &SetOpResult{
			Key:    []byte(k),
			Values: v,
		})
	}
	sort.Sort(testResults(result))
	return
}

func xor(merger mergeFunc, sets ...[]tP) (result []*SetOpResult) {
	hashes := make([]map[string][]byte, len(sets))
	for index, set := range sets {
		hashes[index] = make(map[string][]byte)
		for _, pair := range set {
			hashes[index][pair[0]] = []byte(pair[1])
		}
	}
	resultMap := make(map[string][][]byte)
	for _, m := range hashes {
		for k, v := range m {
			resultMap[k] = merger(resultMap[k], [][]byte{v})
		}
	}
	for k, v := range resultMap {
		if len(v) == 1 {
			result = append(result, &SetOpResult{
				Key:    []byte(k),
				Values: v,
			})
		}
	}
	sort.Sort(testResults(result))
	return
}

func union(merger mergeFunc, sets ...[]tP) (result []*SetOpResult) {
	hashes := make([]map[string][]byte, len(sets))
	for index, set := range sets {
		hashes[index] = make(map[string][]byte)
		for _, pair := range set {
			hashes[index][pair[0]] = []byte(pair[1])
		}
	}
	resultMap := make(map[string][][]byte)
	for _, m := range hashes {
		for k, v := range m {
			resultMap[k] = merger(resultMap[k], [][]byte{v})
		}
	}
	for k, v := range resultMap {
		result = append(result, &SetOpResult{
			Key:    []byte(k),
			Values: v,
		})
	}
	sort.Sort(testResults(result))
	return
}

func TestUnion(t *testing.T) {
	resetSets()
	found := collect(t, "(U a b)")
	expected := union(_append, testSets["a"].pairs, testSets["b"].pairs)
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}

func TestInter(t *testing.T) {
	resetSets()
	found := collect(t, "(I a b)")
	expected := inter(_append, testSets["a"].pairs, testSets["b"].pairs)
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}

func TestDiff(t *testing.T) {
	resetSets()
	found := collect(t, "(D a b)")
	expected := diff(_append, testSets["a"].pairs, testSets["b"].pairs)
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}

func TestXor(t *testing.T) {
	resetSets()
	found := collect(t, "(X a b)")
	expected := xor(_append, testSets["a"].pairs, testSets["b"].pairs)
	if !reflect.DeepEqual(found, expected) {
		t.Errorf("%v should be %v", found, expected)
	}
}

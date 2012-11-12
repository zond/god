package common

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"runtime"
	"sort"
	"testing"
	"time"
)

const (
	Redundancy   = 3
	PingInterval = time.Second
)

func Max64(i ...int64) (result int64) {
	for _, x := range i {
		if x > result {
			result = x
		}
	}
	return
}

func Min64(i ...int64) (result int64) {
	result = i[0]
	for _, x := range i {
		if x < result {
			result = x
		}
	}
	return
}

func Max(i ...int) (result int) {
	for _, x := range i {
		if x > result {
			result = x
		}
	}
	return
}

func Min(i ...int) (result int) {
	result = i[0]
	for _, x := range i {
		if x < result {
			result = x
		}
	}
	return
}

func AssertWithin(t *testing.T, f func() (string, bool), d time.Duration) {
	deadline := time.Now().Add(d)
	var ok bool
	var msg string
	for time.Now().Before(deadline) {
		if msg, ok = f(); ok {
			return
		}
		time.Sleep(time.Second)
	}
	var file string
	var line int
	_, file, line, _ = runtime.Caller(1)
	t.Errorf("%v:%v: Wanted %v to be true within %v, but it never happened: %v", file, line, f, d, msg)
}

func HexEncode(b []byte) (result string) {
	encoded := hex.EncodeToString(b)
	buffer := new(bytes.Buffer)
	for i := len(encoded); i < len(b)*2; i++ {
		fmt.Fprint(buffer, "00")
	}
	fmt.Fprint(buffer, encoded)
	return string(buffer.Bytes())
}

func BetweenII(needle, fromInc, toInc []byte) (result bool) {
	switch bytes.Compare(fromInc, toInc) {
	case 0:
		result = true
	case -1:
		result = bytes.Compare(fromInc, needle) < 1 && bytes.Compare(needle, toInc) < 1
	case 1:
		result = bytes.Compare(fromInc, needle) < 1 || bytes.Compare(needle, toInc) < 1
	default:
		panic("Shouldn't happen")
	}
	return
}
func BetweenIE(needle, fromInc, toExc []byte) (result bool) {
	switch bytes.Compare(fromInc, toExc) {
	case 0:
		result = true
	case -1:
		result = bytes.Compare(fromInc, needle) < 1 && bytes.Compare(needle, toExc) < 0
	case 1:
		result = bytes.Compare(fromInc, needle) < 1 || bytes.Compare(needle, toExc) < 0
	default:
		panic("Shouldn't happen")
	}
	return
}

func MergeItems(arys []*[]Item, up bool) (result []Item) {
	result = *arys[0]
	var items []Item
	for i := 1; i < len(arys); i++ {
		items = *arys[i]
		for _, item := range items {
			i := sort.Search(len(result), func(i int) bool {
				cmp := bytes.Compare(item.Key, result[i].Key)
				if up {
					return cmp < 1
				}
				return cmp > -1
			})
			if i == len(result) {
				result = append(result, item)
			} else {
				if bytes.Compare(result[i].Key, item.Key) == 0 {
					if result[i].Timestamp < item.Timestamp {
						result[i] = item
					}
				} else {
					result = append(result[:i], append([]Item{item}, result[i:]...)...)
				}
			}
		}
	}
	return
}

type DHashDescription struct {
	LastClean    time.Time
	LastSync     time.Time
	LastMigrate  time.Time
	Timer        time.Time
	OwnedEntries int
	HeldEntries  int
	Nodes        Remotes
}

func (self DHashDescription) Describe() string {
	return fmt.Sprintf("%+v", struct {
		LastClean    time.Time
		LastSync     time.Time
		LastMigrate  time.Time
		Timer        time.Time
		OwnedEntries int
		HeldEntries  int
		Nodes        string
	}{
		LastClean:    self.LastClean,
		LastSync:     self.LastSync,
		LastMigrate:  self.LastMigrate,
		Timer:        self.Timer,
		OwnedEntries: self.OwnedEntries,
		HeldEntries:  self.HeldEntries,
		Nodes:        fmt.Sprintf("\n%v", self.Nodes.Describe()),
	})
}

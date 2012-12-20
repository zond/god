package radix

import (
	"../murmur"
	"../persistence"
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"sync"
	"time"
)

type NaiveTimer struct{}

func (self NaiveTimer) ContinuousTime() int64 {
	return time.Now().UnixNano()
}

type TreeIterator func(key, value []byte, timestamp int64) (cont bool)

type TreeIndexIterator func(key, value []byte, timestamp int64, index int) (cont bool)

func cmps(mininc, maxinc bool) (mincmp, maxcmp int) {
	if mininc {
		mincmp = -1
	}
	if maxinc {
		maxcmp = 1
	}
	return
}

func escapeBytes(b []byte) (result []byte) {
	result = make([]byte, 0, len(b))
	for _, c := range b {
		if c == 0 {
			result = append(result, 0, 0)
		} else {
			result = append(result, c)
		}
	}
	return
}

func incrementBytes(b []byte) []byte {
	return new(big.Int).Add(new(big.Int).SetBytes(b), big.NewInt(1)).Bytes()
}

func newMirrorIterator(min, max []byte, mininc, maxinc bool, f TreeIterator) TreeIterator {
	return func(key, value []byte, timestamp int64) bool {
		gt := 0
		if mininc {
			gt = -1
		}
		lt := 0
		if maxinc {
			lt = 1
		}
		k := key[:len(key)-len(escapeBytes(value))-1]
		if (min == nil || bytes.Compare(k, min) > gt) && (max == nil || bytes.Compare(k, max) < lt) {
			return f(k, value, timestamp)
		}
		return true
	}
}

func newMirrorIndexIterator(f TreeIndexIterator) TreeIndexIterator {
	return func(key, value []byte, timestamp int64, index int) bool {
		return f(key[:len(key)-len(escapeBytes(value))-1], value, timestamp, index)
	}
}

func newNodeIterator(f TreeIterator) nodeIterator {
	return func(key, bValue []byte, tValue *Tree, use int, timestamp int64) (cont bool) {
		return f(key, bValue, timestamp)
	}
}

func newNodeIndexIterator(f TreeIndexIterator) nodeIndexIterator {
	return func(key, bValue []byte, tValue *Tree, use int, timestamp int64, index int) (cont bool) {
		return f(key, bValue, timestamp, index)
	}
}

// Tree defines a more specialized wrapper around the node structure.
// It contains an RWMutex to make it thread safe, and it defines a simplified and limited access API.
type Tree struct {
	lock                   *sync.RWMutex
	timer                  Timer
	logger                 *persistence.Logger
	root                   *node
	mirror                 *Tree
	configuration          map[string]string
	configurationTimestamp int64
}

func NewTree() *Tree {
	return NewTreeTimer(NaiveTimer{})
}
func NewTreeTimer(timer Timer) (result *Tree) {
	result = &Tree{
		lock:          new(sync.RWMutex),
		timer:         timer,
		configuration: make(map[string]string),
	}
	result.root, _, _, _, _ = result.root.insert(nil, newNode(nil, nil, nil, 0, true, 0), result.timer.ContinuousTime())
	return
}
func (self *Tree) conf() (result map[string]string, ts int64) {
	result = make(map[string]string)
	for k, v := range self.configuration {
		result[k] = v
	}
	return result, self.configurationTimestamp
}
func (self *Tree) Configuration() (map[string]string, int64) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.conf()
}
func (self *Tree) mirrorClear(timestamp int64) {
	if self.mirror != nil {
		self.mirror.Clear(timestamp)
	}
}
func (self *Tree) mirrorPut(key, value []byte, timestamp int64) {
	if self.mirror != nil {
		escapedKey := escapeBytes(key)
		newKey := make([]byte, len(escapedKey)+len(value)+1)
		copy(newKey, value)
		copy(newKey[len(value)+1:], escapedKey)
		self.mirror.Put(newKey, key, timestamp)
	}
}
func (self *Tree) mirrorFakeDel(key, value []byte, timestamp int64) {
	if self.mirror != nil {
		escapedKey := escapeBytes(key)
		newKey := make([]byte, len(escapedKey)+len(value)+1)
		copy(newKey, value)
		copy(newKey[len(value)+1:], escapedKey)
		self.mirror.FakeDel(newKey, timestamp)
	}
}
func (self *Tree) mirrorDel(key, value []byte) {
	if self.mirror != nil {
		escapedKey := escapeBytes(key)
		newKey := make([]byte, len(escapedKey)+len(value)+1)
		copy(newKey, value)
		copy(newKey[len(value)+1:], escapedKey)
		self.mirror.Del(newKey)
	}
}
func (self *Tree) startMirroring() {
	self.mirror = NewTreeTimer(self.timer)
	self.root.each(nil, byteValue, func(key, byteValue []byte, treeValue *Tree, use int, timestamp int64) bool {
		self.mirrorPut(key, byteValue, timestamp)
		return true
	})
}
func (self *Tree) configure(conf map[string]string, ts int64) {
	if conf[mirrored] == yes && self.configuration[mirrored] != yes {
		self.startMirroring()
	} else if conf[mirrored] != yes && self.configuration[mirrored] == yes {
		self.mirror = nil
	}
	self.configuration = conf
	self.configurationTimestamp = ts
	self.log(persistence.Op{
		Configuration: conf,
		Timestamp:     ts,
	})
}
func (self *Tree) Configure(conf map[string]string, ts int64) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.configure(conf, ts)
}
func (self *Tree) AddConfiguration(ts int64, key, value string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	oldConf, _ := self.conf()
	oldConf[key] = value
	self.configure(oldConf, ts)
}
func (self *Tree) Log(dir string) *Tree {
	self.logger = persistence.NewLogger(dir)
	<-self.logger.Record()
	return self
}
func (self *Tree) Restore() *Tree {
	self.logger.Stop()
	self.logger.Play(func(op persistence.Op) {
		if op.Configuration != nil {
			if op.Key == nil {
				self.Configure(op.Configuration, op.Timestamp)
			} else {
				self.SubConfigure(op.Key, op.Configuration, op.Timestamp)
			}
		} else if op.Put {
			if op.SubKey == nil {
				self.Put(op.Key, op.Value, op.Timestamp)
			} else {
				self.SubPut(op.Key, op.SubKey, op.Value, op.Timestamp)
			}
		} else {
			if op.SubKey == nil {
				if op.Clear {
					self.root, _, _, _, _ = self.root.del(nil, rip(op.Key), treeValue, self.timer.ContinuousTime())
				} else {
					self.Del(op.Key)
				}
			} else {
				self.SubDel(op.Key, op.SubKey)
			}
		}
	})
	<-self.logger.Record()
	return self
}
func (self *Tree) log(op persistence.Op) {
	if self.logger != nil && self.logger.Recording() {
		self.logger.Dump(op)
	}
}
func (self *Tree) newTreeWith(key []Nibble, byteValue []byte, timestamp int64) (result *Tree) {
	result = NewTreeTimer(self.timer)
	result.PutTimestamp(key, byteValue, true, 0, timestamp)
	return
}
func (self *Tree) Each(f TreeIterator) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.each(nil, byteValue, newNodeIterator(f))
}
func (self *Tree) ReverseEach(f TreeIterator) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.reverseEach(nil, byteValue, newNodeIterator(f))
}
func (self *Tree) MirrorEachBetween(min, max []byte, mininc, maxinc bool, f TreeIterator) {
	if self == nil || self.mirror == nil {
		return
	}
	if maxinc && max != nil {
		maxinc = false
		max = incrementBytes(max)
	}
	self.mirror.EachBetween(min, max, mininc, maxinc, newMirrorIterator(min, max, mininc, maxinc, f))
}
func (self *Tree) EachBetween(min, max []byte, mininc, maxinc bool, f TreeIterator) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	mincmp, maxcmp := cmps(mininc, maxinc)
	self.root.eachBetween(nil, rip(min), rip(max), mincmp, maxcmp, byteValue, newNodeIterator(f))
}
func (self *Tree) MirrorReverseEachBetween(min, max []byte, mininc, maxinc bool, f TreeIterator) {
	if self == nil || self.mirror == nil {
		return
	}
	if maxinc && max != nil {
		maxinc = false
		max = incrementBytes(max)
	}
	self.mirror.ReverseEachBetween(min, max, mininc, maxinc, newMirrorIterator(min, max, mininc, maxinc, f))
}
func (self *Tree) ReverseEachBetween(min, max []byte, mininc, maxinc bool, f TreeIterator) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	mincmp, maxcmp := cmps(mininc, maxinc)
	self.root.reverseEachBetween(nil, rip(min), rip(max), mincmp, maxcmp, byteValue, newNodeIterator(f))
}
func (self *Tree) MirrorIndexOf(key []byte) (index int, existed bool) {
	if self == nil || self.mirror == nil {
		return
	}
	var value []byte
	self.MirrorEachBetween(key, nil, true, false, func(k, v []byte, ts int64) bool {
		value, existed = v, bytes.Compare(key, k[:len(key)]) == 0
		return false
	})
	newKey := key
	if existed {
		escapedValue := escapeBytes(value)
		newKey = make([]byte, len(escapedValue)+len(key)+1)
		copy(newKey, key)
		copy(newKey[len(key)+1:], escapedValue)
	}
	index, _ = self.mirror.IndexOf(newKey)
	return
}
func (self *Tree) IndexOf(key []byte) (index int, existed bool) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	index, ex := self.root.indexOf(0, rip(key), byteValue, true)
	existed = ex&byteValue != 0
	return
}
func (self *Tree) MirrorReverseIndexOf(key []byte) (index int, existed bool) {
	if self == nil || self.mirror == nil {
		return
	}
	var value []byte
	self.MirrorEachBetween(key, nil, true, false, func(k, v []byte, ts int64) bool {
		value, existed = v, bytes.Compare(key, k[:len(key)]) == 0
		return false
	})
	newKey := key
	if existed {
		escapedValue := escapeBytes(value)
		newKey = make([]byte, len(escapedValue)+len(key)+1)
		copy(newKey, key)
		copy(newKey[len(key)+1:], escapedValue)
	}
	index, _ = self.mirror.ReverseIndexOf(newKey)
	return
}
func (self *Tree) ReverseIndexOf(key []byte) (index int, existed bool) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	index, ex := self.root.indexOf(0, rip(key), byteValue, false)
	existed = ex&byteValue != 0
	return
}
func (self *Tree) MirrorEachBetweenIndex(min, max *int, f TreeIndexIterator) {
	if self == nil || self.mirror == nil {
		return
	}
	self.mirror.EachBetweenIndex(min, max, newMirrorIndexIterator(f))
}
func (self *Tree) EachBetweenIndex(min, max *int, f TreeIndexIterator) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.eachBetweenIndex(nil, 0, min, max, byteValue, newNodeIndexIterator(f))
}
func (self *Tree) MirrorReverseEachBetweenIndex(min, max *int, f TreeIndexIterator) {
	if self == nil || self.mirror == nil {
		return
	}
	self.mirror.ReverseEachBetweenIndex(min, max, newMirrorIndexIterator(f))
}
func (self *Tree) ReverseEachBetweenIndex(min, max *int, f TreeIndexIterator) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.reverseEachBetweenIndex(nil, 0, min, max, byteValue, newNodeIndexIterator(f))
}
func (self *Tree) Hash() []byte {
	if self == nil {
		return nil
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	hash := murmur.NewString(fmt.Sprint(self.configuration))
	return hash.Sum(self.root.hash)
}
func (self *Tree) ToMap() (result map[string][]byte) {
	if self == nil {
		return
	}
	result = make(map[string][]byte)
	self.Each(func(key []byte, value []byte, timestamp int64) bool {
		result[hex.EncodeToString(key)] = value
		return true
	})
	return
}
func (self *Tree) String() string {
	if self == nil {
		return ""
	}
	return fmt.Sprint(self.ToMap())
}
func (self *Tree) sizeBetween(min, max []byte, mininc, maxinc bool, use int) int {
	if self == nil {
		return 0
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	mincmp, maxcmp := cmps(mininc, maxinc)
	return self.root.sizeBetween(nil, rip(min), rip(max), mincmp, maxcmp, use)
}
func (self *Tree) RealSizeBetween(min, max []byte, mininc, maxinc bool) int {
	return self.sizeBetween(min, max, mininc, maxinc, 0)
}
func (self *Tree) MirrorSizeBetween(min, max []byte, mininc, maxinc bool) (i int) {
	if self == nil || self.mirror != nil {
		return
	}
	return self.mirror.SizeBetween(min, max, mininc, maxinc)
}
func (self *Tree) SizeBetween(min, max []byte, mininc, maxinc bool) int {
	return self.sizeBetween(min, max, mininc, maxinc, byteValue|treeValue)
}
func (self *Tree) RealSize() int {
	if self == nil {
		return 0
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.root.realSize
}
func (self *Tree) Size() int {
	if self == nil {
		return 0
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.root.byteSize + self.root.treeSize
}
func (self *Tree) describeIndented(first, indent int) string {
	if self == nil {
		return ""
	}
	indentation := &bytes.Buffer{}
	for i := 0; i < first; i++ {
		fmt.Fprint(indentation, " ")
	}
	buffer := bytes.NewBufferString(fmt.Sprintf("%v<Radix size:%v hash:%v>\n", indentation, self.Size(), hex.EncodeToString(self.Hash())))
	self.root.describe(indent+2, buffer)
	if self.mirror != nil {
		for i := 0; i < indent+first; i++ {
			fmt.Fprint(buffer, " ")
		}
		fmt.Fprint(buffer, "<mirror>\n")
		self.mirror.root.describe(indent+2, buffer)
	}
	return string(buffer.Bytes())
}
func (self *Tree) Describe() string {
	if self == nil {
		return ""
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.describeIndented(0, 0)
}

func (self *Tree) FakeDel(key []byte, timestamp int64) (oldBytes []byte, oldTree *Tree, existed bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	var ex int
	self.root, oldBytes, oldTree, _, ex = self.root.fakeDel(nil, rip(key), byteValue, timestamp, self.timer.ContinuousTime())
	existed = ex&byteValue != 0
	if existed {
		self.mirrorFakeDel(key, oldBytes, timestamp)
		self.log(persistence.Op{
			Key: key,
		})
	}
	return
}
func (self *Tree) put(key []Nibble, byteValue []byte, treeValue *Tree, use int, timestamp int64) (oldBytes []byte, oldTree *Tree, existed int) {
	self.root, oldBytes, oldTree, _, existed = self.root.insert(nil, newNode(key, byteValue, treeValue, timestamp, false, use), self.timer.ContinuousTime())
	return
}
func (self *Tree) Put(key []byte, bValue []byte, timestamp int64) (oldBytes []byte, existed bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	oldBytes, _, ex := self.put(rip(key), bValue, nil, byteValue, timestamp)
	existed = ex*byteValue != 0
	if existed {
		self.mirrorDel(key, oldBytes)
	}
	self.mirrorPut(key, bValue, timestamp)
	self.log(persistence.Op{
		Key:       key,
		Value:     bValue,
		Timestamp: timestamp,
		Put:       true,
	})
	return
}
func (self *Tree) Get(key []byte) (bValue []byte, timestamp int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	bValue, _, timestamp, ex := self.root.get(rip(key))
	existed = ex&byteValue != 0
	return
}
func (self *Tree) PrevMarker(key []byte) (prevKey []byte, existed bool) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.reverseEachBetween(nil, nil, rip(key), 0, 0, 0, func(k, b []byte, t *Tree, u int, v int64) bool {
		prevKey, existed = k, true
		return false
	})
	return
}
func (self *Tree) NextMarker(key []byte) (nextKey []byte, existed bool) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.eachBetween(nil, rip(key), nil, 0, 0, 0, func(k, b []byte, t *Tree, u int, v int64) bool {
		nextKey, existed = k, true
		return false
	})
	return
}
func (self *Tree) MirrorPrev(key []byte) (prevKey, prevValue []byte, prevTimestamp int64, existed bool) {
	if self == nil || self.mirror == nil {
		return
	}
	prevKey, prevValue, prevTimestamp, existed = self.mirror.Prev(key)
	prevKey = prevKey[:len(prevKey)-len(escapeBytes(prevValue))-1]
	return
}
func (self *Tree) MirrorNext(key []byte) (nextKey, nextValue []byte, nextTimestamp int64, existed bool) {
	if self == nil || self.mirror == nil {
		return
	}
	nextKey, nextValue, nextTimestamp, existed = self.mirror.Next(key)
	nextKey = nextKey[:len(nextKey)-len(escapeBytes(nextValue))-1]
	return
}
func (self *Tree) Prev(key []byte) (prevKey, prevValue []byte, prevTimestamp int64, existed bool) {
	self.ReverseEachBetween(nil, key, false, false, func(k, v []byte, timestamp int64) bool {
		prevKey, prevValue, prevTimestamp, existed = k, v, timestamp, true
		return false
	})
	return
}
func (self *Tree) Next(key []byte) (nextKey, nextValue []byte, nextTimestamp int64, existed bool) {
	self.EachBetween(key, nil, false, false, func(k, v []byte, timestamp int64) bool {
		nextKey, nextValue, nextTimestamp, existed = k, v, timestamp, true
		return false
	})
	return
}
func (self *Tree) NextMarkerIndex(index int) (key []byte, existed bool) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.eachBetweenIndex(nil, 0, &index, nil, 0, func(k, b []byte, t *Tree, u int, v int64, i int) bool {
		key, existed = k, true
		return false
	})
	return
}
func (self *Tree) PrevMarkerIndex(index int) (key []byte, existed bool) {
	if self == nil {
		return
	}
	self.lock.RLock()
	defer self.lock.RUnlock()
	self.root.reverseEachBetweenIndex(nil, 0, nil, &index, 0, func(k, b []byte, t *Tree, u int, v int64, i int) bool {
		key, existed = k, true
		return false
	})
	return
}
func (self *Tree) MirrorNextIndex(index int) (key, value []byte, timestamp int64, ind int, existed bool) {
	if self == nil || self.mirror == nil {
		return
	}
	key, value, timestamp, ind, existed = self.mirror.NextIndex(index)
	key = key[:len(key)-len(escapeBytes(value))-1]
	return
}
func (self *Tree) MirrorPrevIndex(index int) (key, value []byte, timestamp int64, ind int, existed bool) {
	if self == nil || self.mirror == nil {
		return
	}
	key, value, timestamp, ind, existed = self.mirror.PrevIndex(index)
	key = key[:len(key)-len(escapeBytes(value))-1]
	return
}
func (self *Tree) NextIndex(index int) (key, value []byte, timestamp int64, ind int, existed bool) {
	self.EachBetweenIndex(&index, nil, func(k, v []byte, t int64, i int) bool {
		key, value, timestamp, ind, existed = k, v, t, i, true
		return false
	})
	return
}
func (self *Tree) PrevIndex(index int) (key, value []byte, timestamp int64, ind int, existed bool) {
	self.ReverseEachBetweenIndex(nil, &index, func(k, v []byte, t int64, i int) bool {
		key, value, timestamp, ind, existed = k, v, t, i, true
		return false
	})
	return
}
func (self *Tree) MirrorFirst() (key, byteValue []byte, timestamp int64, existed bool) {
	if self == nil || self.mirror == nil {
		return
	}
	key, byteValue, timestamp, existed = self.mirror.First()
	key = key[:len(key)-len(escapeBytes(byteValue))-1]
	return
}
func (self *Tree) MirrorLast() (key, byteValue []byte, timestamp int64, existed bool) {
	if self == nil || self.mirror == nil {
		return
	}
	key, byteValue, timestamp, existed = self.mirror.Last()
	key = key[:len(key)-len(escapeBytes(byteValue))-1]
	return
}
func (self *Tree) First() (key, byteValue []byte, timestamp int64, existed bool) {
	self.Each(func(k []byte, b []byte, ver int64) bool {
		key, byteValue, timestamp, existed = k, b, ver, true
		return false
	})
	return
}
func (self *Tree) Last() (key, byteValue []byte, timestamp int64, existed bool) {
	self.ReverseEach(func(k []byte, b []byte, ver int64) bool {
		key, byteValue, timestamp, existed = k, b, ver, true
		return false
	})
	return
}
func (self *Tree) MirrorIndex(n int) (key, byteValue []byte, timestamp int64, existed bool) {
	if self == nil || self.mirror == nil {
		return
	}
	key, byteValue, timestamp, existed = self.mirror.Index(n)
	key = key[:len(key)-len(escapeBytes(byteValue))-1]
	return
}
func (self *Tree) MirrorReverseIndex(n int) (key, byteValue []byte, timestamp int64, existed bool) {
	if self == nil || self.mirror == nil {
		return
	}
	key, byteValue, timestamp, existed = self.mirror.Index(n)
	key = key[:len(key)-len(escapeBytes(byteValue))-1]
	return
}
func (self *Tree) Index(n int) (key []byte, byteValue []byte, timestamp int64, existed bool) {
	self.EachBetweenIndex(&n, &n, func(k []byte, b []byte, ver int64, index int) bool {
		key, byteValue, timestamp, existed = k, b, ver, true
		return false
	})
	return
}
func (self *Tree) ReverseIndex(n int) (key []byte, byteValue []byte, timestamp int64, existed bool) {
	self.ReverseEachBetweenIndex(&n, &n, func(k []byte, b []byte, ver int64, index int) bool {
		key, byteValue, timestamp, existed = k, b, ver, true
		return false
	})
	return
}
func (self *Tree) Clear(timestamp int64) (result int) {
	self.lock.Lock()
	defer self.lock.Unlock()
	result = self.root.fakeClear(nil, byteValue, timestamp, self.timer.ContinuousTime())
	self.mirrorClear(timestamp)
	if self.logger != nil {
		self.logger.Clear()
	}
	return
}
func (self *Tree) del(key []Nibble) (oldBytes []byte, existed bool) {
	var ex int
	self.root, oldBytes, _, _, ex = self.root.del(nil, key, byteValue, self.timer.ContinuousTime())
	existed = ex&byteValue != 0
	return
}

func (self *Tree) Del(key []byte) (oldBytes []byte, existed bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	oldBytes, existed = self.del(rip(key))
	if existed {
		self.mirrorDel(key, oldBytes)
		self.log(persistence.Op{
			Key: key,
		})
	}
	return
}

func (self *Tree) SubMirrorReverseIndexOf(key, subKey []byte) (index int, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		index, existed = subTree.MirrorReverseIndexOf(subKey)
	}
	return
}
func (self *Tree) SubMirrorIndexOf(key, subKey []byte) (index int, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		index, existed = subTree.MirrorIndexOf(subKey)
	}
	return
}
func (self *Tree) SubReverseIndexOf(key, subKey []byte) (index int, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		index, existed = subTree.ReverseIndexOf(subKey)
	}
	return
}
func (self *Tree) SubIndexOf(key, subKey []byte) (index int, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		index, existed = subTree.IndexOf(subKey)
	}
	return
}
func (self *Tree) SubMirrorPrevIndex(key []byte, index int) (foundKey, foundValue []byte, foundTimestamp int64, foundIndex int, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		foundKey, foundValue, foundTimestamp, foundIndex, existed = subTree.MirrorPrevIndex(index)
	}
	return
}
func (self *Tree) SubMirrorNextIndex(key []byte, index int) (foundKey, foundValue []byte, foundTimestamp int64, foundIndex int, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		foundKey, foundValue, foundTimestamp, foundIndex, existed = subTree.MirrorNextIndex(index)
	}
	return
}
func (self *Tree) SubPrevIndex(key []byte, index int) (foundKey, foundValue []byte, foundTimestamp int64, foundIndex int, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		foundKey, foundValue, foundTimestamp, foundIndex, existed = subTree.PrevIndex(index)
	}
	return
}
func (self *Tree) SubNextIndex(key []byte, index int) (foundKey, foundValue []byte, foundTimestamp int64, foundIndex int, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		foundKey, foundValue, foundTimestamp, foundIndex, existed = subTree.NextIndex(index)
	}
	return
}
func (self *Tree) SubMirrorFirst(key []byte) (firstKey []byte, firstBytes []byte, firstTimestamp int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		firstKey, firstBytes, firstTimestamp, existed = subTree.MirrorFirst()
	}
	return
}
func (self *Tree) SubMirrorLast(key []byte) (lastKey []byte, lastBytes []byte, lastTimestamp int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		lastKey, lastBytes, lastTimestamp, existed = subTree.MirrorLast()
	}
	return
}
func (self *Tree) SubFirst(key []byte) (firstKey []byte, firstBytes []byte, firstTimestamp int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		firstKey, firstBytes, firstTimestamp, existed = subTree.First()
	}
	return
}
func (self *Tree) SubLast(key []byte) (lastKey []byte, lastBytes []byte, lastTimestamp int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		lastKey, lastBytes, lastTimestamp, existed = subTree.Last()
	}
	return
}
func (self *Tree) SubMirrorPrev(key, subKey []byte) (prevKey, prevValue []byte, prevTimestamp int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		prevKey, prevValue, prevTimestamp, existed = subTree.MirrorPrev(subKey)
	}
	return
}
func (self *Tree) SubMirrorNext(key, subKey []byte) (nextKey, nextValue []byte, nextTimestamp int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		nextKey, nextValue, nextTimestamp, existed = subTree.MirrorNext(subKey)
	}
	return
}
func (self *Tree) SubPrev(key, subKey []byte) (prevKey, prevValue []byte, prevTimestamp int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		prevKey, prevValue, prevTimestamp, existed = subTree.Prev(subKey)
	}
	return
}
func (self *Tree) SubNext(key, subKey []byte) (nextKey, nextValue []byte, nextTimestamp int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		nextKey, nextValue, nextTimestamp, existed = subTree.Next(subKey)
	}
	return
}
func (self *Tree) SubSize(key []byte) (result int) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		result = subTree.Size()
	}
	return
}
func (self *Tree) SubMirrorSizeBetween(key, min, max []byte, mininc, maxinc bool) (result int) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		result = subTree.MirrorSizeBetween(min, max, mininc, maxinc)
	}
	return
}
func (self *Tree) SubSizeBetween(key, min, max []byte, mininc, maxinc bool) (result int) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		result = subTree.SizeBetween(min, max, mininc, maxinc)
	}
	return
}
func (self *Tree) SubGet(key, subKey []byte) (byteValue []byte, timestamp int64, existed bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		byteValue, timestamp, existed = subTree.Get(subKey)
	}
	return
}
func (self *Tree) SubMirrorReverseEachBetween(key, min, max []byte, mininc, maxinc bool, f TreeIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		subTree.MirrorReverseEachBetween(min, max, mininc, maxinc, f)
	}
}
func (self *Tree) SubMirrorEachBetween(key, min, max []byte, mininc, maxinc bool, f TreeIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		subTree.MirrorEachBetween(min, max, mininc, maxinc, f)
	}
}
func (self *Tree) SubMirrorReverseEachBetweenIndex(key []byte, min, max *int, f TreeIndexIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		subTree.MirrorReverseEachBetweenIndex(min, max, f)
	}
}
func (self *Tree) SubMirrorEachBetweenIndex(key []byte, min, max *int, f TreeIndexIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		subTree.MirrorEachBetweenIndex(min, max, f)
	}
}
func (self *Tree) SubReverseEachBetween(key, min, max []byte, mininc, maxinc bool, f TreeIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		subTree.ReverseEachBetween(min, max, mininc, maxinc, f)
	}
}
func (self *Tree) SubEachBetween(key, min, max []byte, mininc, maxinc bool, f TreeIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		subTree.EachBetween(min, max, mininc, maxinc, f)
	}
}
func (self *Tree) SubReverseEachBetweenIndex(key []byte, min, max *int, f TreeIndexIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		subTree.ReverseEachBetweenIndex(min, max, f)
	}
}
func (self *Tree) SubEachBetweenIndex(key []byte, min, max *int, f TreeIndexIterator) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		subTree.EachBetweenIndex(min, max, f)
	}
}
func (self *Tree) SubPut(key, subKey []byte, byteValue []byte, timestamp int64) (oldBytes []byte, existed bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	ripped := rip(key)
	_, subTree, subTreeTimestamp, ex := self.root.get(ripped)
	if ex&treeValue == 0 || subTree == nil {
		subTree = self.newTreeWith(rip(subKey), byteValue, timestamp)
	} else {
		oldBytes, existed = subTree.Put(subKey, byteValue, timestamp)
	}
	self.put(ripped, nil, subTree, treeValue, subTreeTimestamp)
	self.log(persistence.Op{
		Key:       key,
		SubKey:    subKey,
		Value:     byteValue,
		Timestamp: timestamp,
		Put:       true,
	})
	return
}
func (self *Tree) SubDel(key, subKey []byte) (oldBytes []byte, existed bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	ripped := rip(key)
	if _, subTree, subTreeTimestamp, ex := self.root.get(ripped); ex&treeValue != 0 && subTree != nil {
		oldBytes, existed = subTree.Del(subKey)
		if subTree.RealSize() == 0 {
			self.del(ripped)
		} else {
			self.put(ripped, nil, subTree, treeValue, subTreeTimestamp)
		}
	}
	if existed {
		self.log(persistence.Op{
			Key:    key,
			SubKey: subKey,
		})
	}
	return
}
func (self *Tree) SubFakeDel(key, subKey []byte, timestamp int64) (oldBytes []byte, existed bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	ripped := rip(key)
	if _, subTree, subTreeTimestamp, ex := self.root.get(ripped); ex&treeValue != 0 && subTree != nil {
		oldBytes, _, existed = subTree.FakeDel(subKey, timestamp)
		self.put(ripped, nil, subTree, treeValue, subTreeTimestamp)
	}
	if existed {
		self.log(persistence.Op{
			Key:    key,
			SubKey: subKey,
		})
	}
	return
}
func (self *Tree) SubClear(key []byte, timestamp int64) (removed int) {
	self.lock.Lock()
	defer self.lock.Unlock()
	ripped := rip(key)
	if _, subTree, subTreeTimestamp, ex := self.root.get(ripped); ex&treeValue != 0 && subTree != nil {
		removed = subTree.Clear(timestamp)
		self.put(ripped, nil, subTree, treeValue, subTreeTimestamp)
	}
	if removed > 0 {
		self.log(persistence.Op{
			Key:   key,
			Clear: true,
		})
	}
	return
}

func (self *Tree) Finger(key []Nibble) *Print {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.root.finger(&Print{}, key)
}
func (self *Tree) GetTimestamp(key []Nibble) (bValue []byte, timestamp int64, present bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	bValue, _, timestamp, ex := self.root.get(key)
	present = ex&byteValue != 0
	return
}
func (self *Tree) putTimestamp(key []Nibble, bValue []byte, treeValue *Tree, nodeUse, insertUse int, expected, timestamp int64) (result bool, oldBytes []byte) {
	if _, _, current, _ := self.root.get(key); current == expected {
		result = true
		self.root, oldBytes, _, _, _ = self.root.insertHelp(nil, newNode(key, bValue, treeValue, timestamp, false, nodeUse), insertUse, self.timer.ContinuousTime())
	}
	return
}
func (self *Tree) PutTimestamp(key []Nibble, bValue []byte, present bool, expected, timestamp int64) (result bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	nodeUse := 0
	if present {
		nodeUse = byteValue
	}
	var oldBytes []byte
	result, oldBytes = self.putTimestamp(key, bValue, nil, nodeUse, byteValue, expected, timestamp)
	if result {
		stitched := stitch(key)
		self.mirrorDel(stitched, oldBytes)
		self.mirrorPut(stitched, bValue, timestamp)
		self.log(persistence.Op{
			Key:       stitch(key),
			Value:     bValue,
			Timestamp: timestamp,
			Put:       true,
		})
	}
	return
}
func (self *Tree) delTimestamp(key []Nibble, use int, expected int64) (result bool, oldBytes []byte) {
	if _, _, current, _ := self.root.get(key); current == expected {
		result = true
		self.root, oldBytes, _, _, _ = self.root.del(nil, key, use, self.timer.ContinuousTime())
	}
	return
}
func (self *Tree) DelTimestamp(key []Nibble, expected int64) (result bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	var oldBytes []byte
	result, oldBytes = self.delTimestamp(key, byteValue, expected)
	if result {
		self.mirrorDel(stitch(key), oldBytes)
		self.log(persistence.Op{
			Key: stitch(key),
		})
	}
	return
}

func (self *Tree) subConfiguration(key []byte) (conf map[string]string, timestamp int64) {
	if _, subTree, _, ex := self.root.get(rip(key)); ex&treeValue != 0 && subTree != nil {
		conf, timestamp = subTree.Configuration()
	} else {
		conf = make(map[string]string)
	}
	return
}
func (self *Tree) SubConfiguration(key []byte) (conf map[string]string, timestamp int64) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	return self.subConfiguration(key)
}
func (self *Tree) subConfigure(key []byte, conf map[string]string, timestamp int64) {
	ripped := rip(key)
	_, subTree, subTreeTimestamp, ex := self.root.get(ripped)
	if ex&treeValue == 0 || subTree == nil {
		subTree = NewTreeTimer(self.timer)
	}
	subTree.Configure(conf, timestamp)
	self.put(ripped, nil, subTree, treeValue, subTreeTimestamp)
	self.log(persistence.Op{
		Key:           key,
		Configuration: conf,
		Timestamp:     timestamp,
	})
	return
}
func (self *Tree) SubConfigure(key []byte, conf map[string]string, timestamp int64) {
	self.lock.Lock()
	defer self.lock.Unlock()
	self.subConfigure(key, conf, timestamp)
}
func (self *Tree) SubAddConfiguration(treeKey []byte, ts int64, key, value string) {
	self.lock.Lock()
	defer self.lock.Unlock()
	oldConf, _ := self.subConfiguration(treeKey)
	oldConf[key] = value
	self.subConfigure(treeKey, oldConf, ts)
	return
}
func (self *Tree) SubFinger(key, subKey []Nibble) (result *Print) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(key); ex&treeValue != 0 && subTree != nil {
		result = subTree.Finger(subKey)
	} else {
		result = &Print{}
	}
	return
}
func (self *Tree) SubGetTimestamp(key, subKey []Nibble) (byteValue []byte, timestamp int64, present bool) {
	self.lock.RLock()
	defer self.lock.RUnlock()
	if _, subTree, _, ex := self.root.get(key); ex&treeValue != 0 && subTree != nil {
		byteValue, timestamp, present = subTree.GetTimestamp(subKey)
	}
	return
}
func (self *Tree) SubPutTimestamp(key, subKey []Nibble, bValue []byte, present bool, subExpected, subTimestamp int64) (result bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	_, subTree, subTreeTimestamp, _ := self.root.get(key)
	if subTree == nil {
		result = true
		subTree = self.newTreeWith(subKey, bValue, subTimestamp)
	} else {
		result = subTree.PutTimestamp(subKey, bValue, present, subExpected, subTimestamp)
	}
	self.putTimestamp(key, nil, subTree, treeValue, treeValue, subTreeTimestamp, subTreeTimestamp)
	if result {
		self.log(persistence.Op{
			Key:       stitch(key),
			SubKey:    stitch(subKey),
			Value:     bValue,
			Timestamp: subTimestamp,
			Put:       true,
		})
	}
	return
}
func (self *Tree) SubDelTimestamp(key, subKey []Nibble, subExpected int64) (result bool) {
	self.lock.Lock()
	defer self.lock.Unlock()
	if _, subTree, subTreeTimestamp, ex := self.root.get(key); ex&treeValue != 0 && subTree != nil {
		result = subTree.DelTimestamp(subKey, subExpected)
		if subTree.Size() == 0 {
			self.delTimestamp(key, treeValue, subTreeTimestamp)
		} else {
			self.putTimestamp(key, nil, subTree, treeValue, treeValue, subTreeTimestamp, subTreeTimestamp)
		}
	}
	if result {
		self.log(persistence.Op{
			Key:    stitch(key),
			SubKey: stitch(subKey),
		})
	}
	return
}

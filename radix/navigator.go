package radix

const (
	spooling = iota
	iterating
	done
)

type Navigator struct {
	tree    *Tree
	reverse bool
	offset  int
	limit   int
	from    []byte
	to      []byte
	hits    int
}

func (self *Navigator) Reverse() *Navigator {
	self.reverse = true
	return self
}
func (self *Navigator) Offset(o int) *Navigator {
	self.offset = o
	return self
}
func (self *Navigator) Limit(n int) *Navigator {
	self.limit = n
	return self
}
func (self *Navigator) From(key []byte) *Navigator {
	self.from = key
	return self
}
func (self *Navigator) To(key []byte) *Navigator {
	self.to = key
	return self
}
func (self *Navigator) findStart() {

}
func (self *Navigator) next() (key []byte, value Hasher, exists bool) {
	return
}
func (self *Navigator) outsideScope(key []byte) bool {
	return false
}
func (self *Navigator) Each(f TreeIterator) {
	self.tree.lock.RLock()
	defer self.tree.lock.RUnlock()
	self.findStart()
	nextKey, nextValue, exists := self.next()
	for exists {
		if self.outsideScope(nextKey) {
			break
		}
		self.tree.lock.RUnlock()
		self.hits++
		f(nextKey, nextValue)
		self.tree.lock.RLock()
		nextKey, nextValue, exists = self.next()
	}
}

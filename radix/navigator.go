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
	state   int
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
func (self *Navigator) Run(f TreeIterator) {
	self.tree.iterate(self, f)
}
func (self *Navigator) register() {
	self.hits++
}
func (self *Navigator) validChild(n *node) bool {
	return true
}
func (self *Navigator) validNode(n *node) bool {
	return true
}

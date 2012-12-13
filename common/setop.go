package common

const (
	Union = iota
	Intersection
	Difference
)

type SetOp struct {
	Sources []interface{}
	Type    int
}

type SetExpression struct {
	Op     SetOp
	Min    []byte
	Max    []byte
	MinInc bool
	MaxInc bool
	Len    int
}

type SetOpResult struct {
	Key    []byte
	Values [][]byte
}

func (self *SetOpResult) ShallowCopy() (result *SetOpResult) {
	result = &SetOpResult{
		Key:    self.Key,
		Values: make([][]byte, len(self.Values)),
	}
	copy(result.Values, self.Values)
	return
}

package common

import (
	"fmt"
	"strings"
)

const (
	Union = iota
	Intersection
	Difference
	Xor
)

type SetOpType int

func (self SetOpType) String() string {
	switch self {
	case Union:
		return "U"
	case Intersection:
		return "I"
	case Difference:
		return "D"
	case Xor:
		return "X"
	}
	panic(fmt.Errorf("Unknown SetOpType %v", self))
}

type SetOpSource struct {
	Key   []byte
	SetOp *SetOp
}

type SetOp struct {
	Sources []SetOpSource
	Type    SetOpType
}

func (self SetOp) String() string {
	sources := make([]string, len(self.Sources))
	for index, source := range self.Sources {
		if source.Key != nil {
			sources[index] = string(source.Key)
		} else {
			sources[index] = fmt.Sprint(source.SetOp)
		}
	}
	return fmt.Sprintf("(%v %v)", self.Type, strings.Join(sources, " "))
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

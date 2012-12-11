package common

type SetOp struct {
	Key1   []byte
	Key2   []byte
	Min    []byte
	Max    []byte
	MinInc bool
	MaxInc bool
	Len    int
}

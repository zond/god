package common

type SetOp struct {
	Keys   [][]byte
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

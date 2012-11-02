package common

type Range struct {
	FromKey   []byte
	ToKey     []byte
	FromInc   bool
	ToInc     bool
	FromIndex int
	ToIndex   int
}

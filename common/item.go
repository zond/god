package common

import (
	"../radix"
)

type Item struct {
	Key       []byte
	Value     radix.Hasher
	Exists    bool
	Timestamp int64
	TTL       int
}

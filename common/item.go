package common

type Item struct {
	Key       []byte
	Value     []byte
	Exists    bool
	Timestamp int64
	TTL       int
}

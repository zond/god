package common

type ConfItem struct {
	TreeKey []byte
	Key     string
	Value   string
}

type Conf struct {
	TreeKey   []byte
	Data      map[string]string
	Timestamp int64
}

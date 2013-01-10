package main

import (
	"fmt"
	"github.com/zond/god/client"
)

func main() {
	conn := client.MustConn("localhost:9191")
	key := []byte("mail@domain.tld/followers")
	conn.SubPut(key, []byte("follower1@domain.tld"), nil)
	conn.SubPut(key, []byte("follower2@domain.tld"), nil)
	conn.SubPut(key, []byte("follower3@domain.tld"), nil)
	fmt.Printf("my first follower is %+v\n", string(conn.SliceLen(key, nil, true, 1)[0].Key))
	last2 := conn.ReverseSliceLen(key, nil, true, 2)
	fmt.Printf("my last two followers are %+v and %+v\n", string(last2[1].Key), string(last2[0].Key))
}

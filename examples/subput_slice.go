package main

import (
  "fmt"
  "github.com/zond/god/client"
)

func main() {
  conn := client.MustConn("localhost:9191")
  // make up a key
  key := []byte("mail@domain.tld/followers")
  // dump sub keys into it
  conn.SubPut(key, []byte("follower1@domain.tld"), nil)
  conn.SubPut(key, []byte("follower2@domain.tld"), nil)
  conn.SubPut(key, []byte("follower3@domain.tld"), nil)
  // and fetch bits and pieces of it
  fmt.Printf("my first follower is %+v\n", string(conn.SliceLen(key, nil, true, 1)[0].Key))
  last2 := conn.ReverseSliceLen(key, nil, true, 2)
  fmt.Printf("my last two followers are %+v and %+v\n", string(last2[1].Key), string(last2[0].Key))
}

// output: my first follower is follower1@domain.tld
// output: my last two followers are user2@domain.tld and user3@domain.tld

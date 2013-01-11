package main

import (
  "fmt"
  "github.com/zond/god/client"
  "github.com/zond/god/common"
)

func main() {
  conn := client.MustConn("localhost:9191")
  key := []byte("score_by_email")
  conn.SubAddConfiguration(key, "mirrored", "yes")
  conn.SubPut(key, []byte("mail1@domain.tld"), common.EncodeInt64(414))
  conn.SubPut(key, []byte("mail2@domain.tld"), common.EncodeInt64(12))
  conn.SubPut(key, []byte("mail3@domain.tld"), common.EncodeInt64(9912))
  conn.SubPut(key, []byte("mail4@domain.tld"), common.EncodeInt64(33))
  conn.SubPut(key, []byte("mail5@domain.tld"), common.EncodeInt64(511))
  conn.SubPut(key, []byte("mail6@domain.tld"), common.EncodeInt64(4512))
  conn.SubPut(key, []byte("mail7@domain.tld"), common.EncodeInt64(1023))
  conn.SubPut(key, []byte("mail8@domain.tld"), common.EncodeInt64(121))
  fmt.Println("top three scores:")
  for index, user := range conn.MirrorReverseSliceLen(key, nil, true, 3) {
    fmt.Println(index, string(user.Value), common.MustDecodeInt64(user.Key))
  }
}

// output: top three scores:
// output: 0 mail3@domain.tld 9912
// output: 1 mail6@domain.tld 4512
// output: 2 mail7@domain.tld 1023

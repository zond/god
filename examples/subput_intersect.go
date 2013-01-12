package main

import (
  "fmt"
  "github.com/zond/god/client"
  "github.com/zond/god/setop"
)

func main() {
  conn := client.MustConn("localhost:9191")
  // lets store followers here
  followersKey := []byte("mail@domain.tld/followers")
  // and followees here
  followeesKey := []byte("mail@domain.tld/followees")
  // create a few of each
  conn.SubPut(followersKey, []byte("user1@domain.tld"), nil)
  conn.SubPut(followersKey, []byte("user2@domain.tld"), nil)
  conn.SubPut(followersKey, []byte("user3@domain.tld"), nil)
  conn.SubPut(followeesKey, []byte("user3@domain.tld"), nil)
  conn.SubPut(followeesKey, []byte("user4@domain.tld"), nil)
  // and fetch the intersection!
  for _, friend := range conn.SetExpression(setop.SetExpression{
    Code: fmt.Sprintf("(I %v %v)", string(followersKey), string(followeesKey)),
  }) {
    fmt.Println(string(friend.Key))
  }
}

// output: user3@domain.tld

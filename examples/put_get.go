package main

import (
  "fmt"
  "github.com/zond/god/client"
  "github.com/zond/god/common"
  "github.com/zond/god/murmur"
)

// a make believe user data structure
type User struct {
  Email    string
  Password string
  Name     string
}

func main() {
  // connect to the default local server
  conn := client.MustConn("localhost:9191")
  // create a user
  user := User{
    Email:    "mail@domain.tld",
    Password: "so secret",
    Name:     "john doe",
  }
  // serialize the user
  bytes := common.MustJSONEncode(user)
  // and put it in the database
  conn.Put(murmur.HashString(user.Email), bytes)
  // try to fetch the user again
  data, _ := conn.Get(murmur.HashString(user.Email))
  var found User
  // to unserialize it
  common.MustJSONDecode(data, &found)
  fmt.Printf("stored and found %+v\n", found)
}

// output: stored and found {Email:mail@domain.tld Password:so secret Name:john doe}

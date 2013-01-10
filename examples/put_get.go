package main

import (
	"encoding/json"
	"fmt"
	"github.com/zond/god/client"
	"github.com/zond/god/murmur"
)

type User struct {
	Email    string
	Password string
	Name     string
}

func main() {
	conn := client.MustConn("localhost:9191")
	user := User{
		Email:    "mail@domain.tld",
		Password: "so secret",
		Name:     "john doe",
	}
	if bytes, err := json.Marshal(user); err != nil {
		panic(err)
	} else {
		conn.Put(murmur.HashString(user.Email), bytes)
	}
	data, _ := conn.Get(murmur.HashString(user.Email))
	var found User
	if err := json.Unmarshal(data, &found); err != nil {
		panic(err)
	}
	fmt.Printf("stored and found %+v\n", found)
}

// output: stored and found {Email:mail@domain.tld Password:so secret Name:john doe}

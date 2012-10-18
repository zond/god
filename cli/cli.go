package main

import (
	"../client"
	"fmt"
)

func main() {
	conn := client.MustConn("127.0.0.1:9191")
	fmt.Println(conn.Describe())
}

package main

import (
	"../client"
	"flag"
	"fmt"
)

var ip = flag.String("ip", "127.0.0.1", "IP address to connect to")
var port = flag.Int("port", 9191, "Port to connect to")

func main() {
	conn := client.MustConn(fmt.Sprintf("%v:%v", *ip, *port))
	fmt.Println(conn.Describe())
}

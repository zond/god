package main

import (
	"../dhash"
	"flag"
	"fmt"
	"time"
)

var ip = flag.String("ip", "127.0.0.1", "IP address to listen to")
var port = flag.Int("port", 9191, "Port to listen to")
var joinIp = flag.String("joinIp", "", "IP address to join")
var joinPort = flag.Int("joinPort", 9191, "Port to join")

func main() {
	flag.Parse()
	s := dhash.NewDHash(fmt.Sprintf("%v:%v", *ip, *port))
	s.MustStart()
	if *joinIp != "" {
		s.MustJoin(fmt.Sprintf("%v:%v", *joinIp, *joinPort))
	}

	for {
		fmt.Println(time.Now(), s.Describe())
		time.Sleep(time.Second * 10)
	}
}

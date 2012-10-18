package main

import (
	"../client"
	"flag"
	"fmt"
	"regexp"
	"strings"
)

type action func(conn *client.Conn, args []string)

var ip = flag.String("ip", "127.0.0.1", "IP address to connect to")
var port = flag.Int("port", 9191, "Port to connect to")

var actions = map[*regexp.Regexp]action{
	regexp.MustCompile("^put (\\S+) (\\S+)$"): put,
	regexp.MustCompile("^$"):                  show,
}

func show(conn *client.Conn, args []string) {
	fmt.Println(conn.Describe())
}

func put(conn *client.Conn, args []string) {
	old, existed, err := conn.Put([]byte(args[1]), []byte(args[2]))
	if err != nil {
		fmt.Println(err)
	} else if existed {
		fmt.Println(old)
	}
}

func main() {
	flag.Parse()
	conn := client.MustConn(fmt.Sprintf("%v:%v", *ip, *port))
	args := strings.Join(flag.Args(), " ")
	for reg, fun := range actions {
		if matches := reg.FindStringSubmatch(args); matches != nil {
			fun(conn, matches)
		}
	}
}

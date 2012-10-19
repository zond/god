package main

import (
	"../client"
	"encoding/hex"
	"flag"
	"fmt"
	"regexp"
	"strings"
)

type action func(conn *client.Conn, args []string)

var ip = flag.String("ip", "127.0.0.1", "IP address to connect to")
var port = flag.Int("port", 9191, "Port to connect to")

var actions = map[*regexp.Regexp]action{
	regexp.MustCompile("^put (\\S+) (\\S+)$"):   put,
	regexp.MustCompile("^$"):                    show,
	regexp.MustCompile("^describeTree (\\S+)$"): describeTree,
	regexp.MustCompile("^get (\\S+)$"):          get,
}

func show(conn *client.Conn, args []string) {
	fmt.Println(conn.Describe())
}

func describeTree(conn *client.Conn, args []string) {
	if bytes, err := hex.DecodeString(args[1]); err != nil {
		fmt.Println(err)
	} else {
		if result, err := conn.DescribeTree(bytes); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(result)
		}
	}
}

func get(conn *client.Conn, args []string) {
	value, existed := conn.Get([]byte(args[1]))
	if existed {
		fmt.Println(string(value))
	}
}

func put(conn *client.Conn, args []string) {
	conn.Put([]byte(args[1]), []byte(args[2]))
}

func main() {
	flag.Parse()
	conn := client.MustConn(fmt.Sprintf("%v:%v", *ip, *port))
	args := strings.Join(flag.Args(), " ")
	for reg, fun := range actions {
		if matches := reg.FindStringSubmatch(args); matches != nil {
			fun(conn, matches)
			return
		}
	}
	fmt.Println("No command given?")
}

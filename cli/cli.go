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
	regexp.MustCompile("^put (\\S+) (\\S+)$"):            put,
	regexp.MustCompile("^get (\\S+)$"):                   get,
	regexp.MustCompile("^del (\\S+)$"):                   del,
	regexp.MustCompile("^subPut (\\S+) (\\S+) (\\S+)$"):  subPut,
	regexp.MustCompile("^subGet (\\S+) (\\S+)$"):         subGet,
	regexp.MustCompile("^subDel (\\S+) (\\S+)$"):         subDel,
	regexp.MustCompile("^$"):                             show,
	regexp.MustCompile("^describeTree (\\S+)$"):          describeTree,
	regexp.MustCompile("^next (\\S+)$"):                  next,
	regexp.MustCompile("^prev (\\S+)$"):                  prev,
	regexp.MustCompile("^subNext (\\S+) (\\S+)$"):        subNext,
	regexp.MustCompile("^subPrev (\\S+) (\\S+)$"):        subPrev,
	regexp.MustCompile("^indexOf (\\S+) (\\S+)$"):        indexOf,
	regexp.MustCompile("^reverseIndexOf (\\S+) (\\S+)$"): reverseIndexOf,
}

func reverseIndexOf(conn *client.Conn, args []string) {
	if index, existed := conn.ReverseIndexOf([]byte(args[1]), []byte(args[2])); existed {
		fmt.Println(index)
	}
}

func indexOf(conn *client.Conn, args []string) {
	if index, existed := conn.IndexOf([]byte(args[1]), []byte(args[2])); existed {
		fmt.Println(index)
	}
}

func show(conn *client.Conn, args []string) {
	fmt.Println(conn.Describe())
}

func prev(conn *client.Conn, args []string) {
	if key, value, existed := conn.Prev([]byte(args[1])); existed {
		fmt.Println(string(key), "=>", string(value))
	}
}

func next(conn *client.Conn, args []string) {
	if key, value, existed := conn.Next([]byte(args[1])); existed {
		fmt.Println(string(key), "=>", string(value))
	}
}

func subNext(conn *client.Conn, args []string) {
	if key, value, existed := conn.SubNext([]byte(args[1]), []byte(args[2])); existed {
		fmt.Println(string(key), "=>", string(value))
	}
}

func subPrev(conn *client.Conn, args []string) {
	if key, value, existed := conn.SubPrev([]byte(args[1]), []byte(args[2])); existed {
		fmt.Println(string(key), "=>", string(value))
	}
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
	if value, existed := conn.Get([]byte(args[1])); existed && value != nil {
		fmt.Println(string(value))
	}
}

func subGet(conn *client.Conn, args []string) {
	if value, existed := conn.SubGet([]byte(args[1]), []byte(args[2])); existed && value != nil {
		fmt.Println(string(value))
	}
}

func put(conn *client.Conn, args []string) {
	conn.Put([]byte(args[1]), []byte(args[2]))
}

func subPut(conn *client.Conn, args []string) {
	conn.SubPut([]byte(args[1]), []byte(args[2]), []byte(args[3]))
}

func subDel(conn *client.Conn, args []string) {
	conn.SubDel([]byte(args[1]), []byte(args[2]))
}

func del(conn *client.Conn, args []string) {
	conn.Del([]byte(args[1]))
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

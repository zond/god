package main

import (
	"../client"
	"../common"
	"encoding/hex"
	"flag"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type action func(conn *client.Conn, args []string)

var ip = flag.String("ip", "127.0.0.1", "IP address to connect to")
var port = flag.Int("port", 9191, "Port to connect to")

type actionSpec struct {
	cmd  string
	args []*regexp.Regexp
}

func newActionSpec(pattern string) (result *actionSpec) {
	result = &actionSpec{}
	parts := strings.Split(pattern, " ")
	result.cmd = parts[0]
	for _, r := range parts[1:] {
		result.args = append(result.args, regexp.MustCompile(r))
	}
	return
}

var actions = map[*actionSpec]action{
	newActionSpec("reverseSliceIndex \\S+ \\d+ \\d+"): reverseSliceIndex,
	newActionSpec("sliceIndex \\S+ \\d+ \\d+"):        sliceIndex,
	newActionSpec("reverseSlice \\S+ \\S+ \\S+"):      reverseSlice,
	newActionSpec("slice \\S+ \\S+ \\S+"):             slice,
	newActionSpec("put \\S+ \\S+"):                    put,
	newActionSpec("count \\S+ \\S+ \\S+"):             count,
	newActionSpec("get \\S+"):                         get,
	newActionSpec("del \\S+"):                         del,
	newActionSpec("subPut \\S+ \\S+ \\S+"):            subPut,
	newActionSpec("subGet \\S+ \\S+"):                 subGet,
	newActionSpec("subDel \\S+ \\S+"):                 subDel,
	newActionSpec("describeAll"):                      describeAll,
	newActionSpec("describe \\S+"):                    describe,
	newActionSpec("describeAllTrees"):                 describeAllTrees,
	newActionSpec("first \\S+"):                       first,
	newActionSpec("last \\S+"):                        last,
	newActionSpec("migrate \\S+"):                     migrate,
	newActionSpec("prevIndex \\S+ \\d+"):              prevIndex,
	newActionSpec("nextIndex \\S+ \\d+"):              nextIndex,
	newActionSpec("next \\S+"):                        next,
	newActionSpec("prev \\S+"):                        prev,
	newActionSpec("subNext \\S+ \\S+"):                subNext,
	newActionSpec("subPrev \\S+ \\S+"):                subPrev,
	newActionSpec("indexOf \\S+ \\S+"):                indexOf,
	newActionSpec("reverseIndexOf \\S+ \\S+"):         reverseIndexOf,
}

func mustAtoi(s string) *int {
	i, err := strconv.Atoi(s)
	if err != nil {
		panic(err)
	}
	return &i
}

func reverseSliceIndex(conn *client.Conn, args []string) {
	for _, item := range conn.ReverseSliceIndex([]byte(args[1]), mustAtoi(args[2]), mustAtoi(args[3])) {
		fmt.Printf("%v: %v => %v\n", item.Index, string(item.Key), string(item.Value))
	}
}

func sliceIndex(conn *client.Conn, args []string) {
	for _, item := range conn.SliceIndex([]byte(args[1]), mustAtoi(args[2]), mustAtoi(args[3])) {
		fmt.Printf("%v: %v => %v\n", item.Index, string(item.Key), string(item.Value))
	}
}

func reverseSlice(conn *client.Conn, args []string) {
	for i, item := range conn.ReverseSlice([]byte(args[1]), []byte(args[2]), []byte(args[3]), true, false) {
		fmt.Printf("%v: %v => %v\n", i, string(item.Key), string(item.Value))
	}
}

func slice(conn *client.Conn, args []string) {
	for i, item := range conn.Slice([]byte(args[1]), []byte(args[2]), []byte(args[3]), true, false) {
		fmt.Printf("%v: %v => %v\n", i, string(item.Key), string(item.Value))
	}
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

func show(conn *client.Conn) {
	fmt.Println(conn.Describe())
}

func prev(conn *client.Conn, args []string) {
	if key, value, existed := conn.Prev([]byte(args[1])); existed {
		fmt.Println(string(key), "=>", string(value))
	}
}

func count(conn *client.Conn, args []string) {
	fmt.Println(conn.Count([]byte(args[1]), []byte(args[2]), []byte(args[3]), true, false))
}

func prevIndex(conn *client.Conn, args []string) {
	if key, value, index, existed := conn.PrevIndex([]byte(args[1]), *(mustAtoi(args[2]))); existed {
		fmt.Printf("%v: %v => %v\n", index, string(key), string(value))
	}
}

func nextIndex(conn *client.Conn, args []string) {
	if key, value, index, existed := conn.NextIndex([]byte(args[1]), *(mustAtoi(args[2]))); existed {
		fmt.Printf("%v: %v => %v\n", index, string(key), string(value))
	}
}

func next(conn *client.Conn, args []string) {
	if key, value, existed := conn.Next([]byte(args[1])); existed {
		fmt.Println(string(key), "=>", string(value))
	}
}

func first(conn *client.Conn, args []string) {
	if key, value, existed := conn.First([]byte(args[1])); existed {
		fmt.Println(string(key), "=>", string(value))
	}
}

func last(conn *client.Conn, args []string) {
	if key, value, existed := conn.Last([]byte(args[1])); existed {
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

func describeAll(conn *client.Conn, args []string) {
	for ind, r := range conn.Nodes() {
		var result common.DHashDescription
		if err := r.Call("DHash.Describe", 0, &result); err != nil {
			fmt.Printf("%v: %v: %v\n", ind, r, err)
		} else {
			fmt.Println(result.Describe())
		}
	}
}

func describeAllTrees(conn *client.Conn, args []string) {
	for ind, r := range conn.Nodes() {
		var result string
		if err := r.Call("DHash.DescribeTree", 0, &result); err != nil {
			fmt.Printf("%v: %v: %v\n", ind, r, err)
		} else {
			fmt.Println(r)
			fmt.Println(result)
		}
	}
}

func migrate(conn *client.Conn, args []string) {
	if bytes, err := hex.DecodeString(args[1]); err != nil {
		fmt.Println(err)
	} else {
		if err := conn.Migrate(bytes); err != nil {
			fmt.Println(err)
		} else {
			describe(conn, args)
		}
	}
}

func describe(conn *client.Conn, args []string) {
	if bytes, err := hex.DecodeString(args[1]); err != nil {
		fmt.Println(err)
	} else {
		if result, err := conn.DescribeNode(bytes); err != nil {
			fmt.Println(err)
		} else {
			fmt.Println(result.Describe())
		}
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
	if len(flag.Args()) == 0 {
		show(conn)
	} else {
		for spec, fun := range actions {
			if spec.cmd == flag.Args()[0] {
				matchingParts := true
				for index, reg := range spec.args {
					if !reg.MatchString(flag.Args()[index+1]) {
						matchingParts = false
						break
					}
				}
				if matchingParts {
					fun(conn, flag.Args())
					return
				}
			}
		}
		fmt.Println("No command given?")
	}
}

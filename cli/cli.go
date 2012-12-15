package main

import (
	"../client"
	"../common"
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
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
	newActionSpec("sliceLen \\S+ \\S+ \\d+"):          sliceLen,
	newActionSpec("setOp .+"):                         setOp,
	newActionSpec("reverseSliceLen \\S+ \\S+ \\d+"):   reverseSliceLen,
	newActionSpec("put \\S+ \\S+"):                    put,
	newActionSpec("dump"):                             dump,
	newActionSpec("subDump \\S+"):                     subDump,
	newActionSpec("subSize \\S+"):                     subSize,
	newActionSpec("size"):                             size,
	newActionSpec("count \\S+ \\S+ \\S+"):             count,
	newActionSpec("get \\S+"):                         get,
	newActionSpec("del \\S+"):                         del,
	newActionSpec("subPut \\S+ \\S+ \\S+"):            subPut,
	newActionSpec("subGet \\S+ \\S+"):                 subGet,
	newActionSpec("subDel \\S+ \\S+"):                 subDel,
	newActionSpec("subClear \\S+"):                    subClear,
	newActionSpec("describeAll"):                      describeAll,
	newActionSpec("describe \\S+"):                    describe,
	newActionSpec("describeTree \\S+"):                describeTree,
	newActionSpec("describeAllTrees"):                 describeAllTrees,
	newActionSpec("first \\S+"):                       first,
	newActionSpec("last \\S+"):                        last,
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

func subSize(conn *client.Conn, args []string) {
	fmt.Println(conn.SubSize([]byte(args[1])))
}

func size(conn *client.Conn, args []string) {
	fmt.Println(conn.Size())
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

func sliceLen(conn *client.Conn, args []string) {
	for _, item := range conn.SliceLen([]byte(args[1]), []byte(args[2]), true, *(mustAtoi(args[3]))) {
		fmt.Printf("%v => %v\n", string(item.Key), string(item.Value))
	}
}

func reverseSliceLen(conn *client.Conn, args []string) {
	for _, item := range conn.ReverseSliceLen([]byte(args[1]), []byte(args[2]), true, *(mustAtoi(args[3]))) {
		fmt.Printf("%v => %v\n", string(item.Key), string(item.Value))
	}
}

func printSetOpRes(res common.SetOpResult) {
	var vals []string
	for _, val := range res.Values {
		vals = append(vals, string(val))
	}
	fmt.Printf("%v => %v\n", string(res.Key), vals)
}

func setOp(conn *client.Conn, args []string) {
	op, err := common.NewSetOpParser(args[1]).Parse()
	if err != nil {
		fmt.Println(err)
	} else {
		for _, res := range conn.SetExpression(common.SetExpression{Op: *op}) {
			printSetOpRes(res)
		}
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

func prev(conn *client.Conn, args []string) {
	if key, value, existed := conn.Prev([]byte(args[1])); existed {
		fmt.Printf("%v => %v", string(key), string(value))
	}
}

func next(conn *client.Conn, args []string) {
	if key, value, existed := conn.Next([]byte(args[1])); existed {
		fmt.Printf("%v => %v", string(key), string(value))
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
		fmt.Printf("%v => %v", string(key), string(value))
	}
}

func subPrev(conn *client.Conn, args []string) {
	if key, value, existed := conn.SubPrev([]byte(args[1]), []byte(args[2])); existed {
		fmt.Printf("%v => %v", string(key), string(value))
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
	if value, existed := conn.Get([]byte(args[1])); existed {
		fmt.Printf("%v\n", string(value))
	}
}

func subGet(conn *client.Conn, args []string) {
	if value, existed := conn.SubGet([]byte(args[1]), []byte(args[2])); existed {
		fmt.Printf("%v\n", string(value))
	}
}

func dump(conn *client.Conn, args []string) {
	dump, wait := conn.Dump()
	linedump(dump, wait)
}

func subDump(conn *client.Conn, args []string) {
	dump, wait := conn.SubDump([]byte(args[1]))
	linedump(dump, wait)
}

func linedump(dump chan [2][]byte, wait *sync.WaitGroup) {
	defer func() {
		close(dump)
		wait.Wait()
	}()
	reader := bufio.NewReader(os.Stdin)
	var pair []string
	var line string
	var err error
	for line, err = reader.ReadString('\n'); err == nil; line, err = reader.ReadString('\n') {
		pair = strings.Split(strings.TrimSpace(line), "=")
		if len(pair) == 2 {
			dump <- [2][]byte{[]byte(pair[0]), []byte(pair[1])}
		} else {
			return
		}
	}
	if err != io.EOF {
		fmt.Println(err)
	}
}

func put(conn *client.Conn, args []string) {
	conn.Put([]byte(args[1]), []byte(args[2]))
}

func subPut(conn *client.Conn, args []string) {
	conn.SubPut([]byte(args[1]), []byte(args[2]), []byte(args[3]))
}

func subClear(conn *client.Conn, args []string) {
	conn.SubClear([]byte(args[1]))
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

package client

import (
	"fmt"

	"github.com/zond/god/client"
	"github.com/zond/god/dhash"
	"github.com/zond/setop"
)

var ran = false

func ExampleSetExpression() {
	server := dhash.NewNodeDir("127.0.0.1:2020", "127.0.0.1:2020", "").MustStart()
	defer server.Stop()
	conn := client.MustConn("127.0.0.1:2020")
	conn.SubPut([]byte("myfriends"), []byte("alice"), setop.EncodeFloat64(10))
	conn.SubPut([]byte("myfriends"), []byte("bob"), setop.EncodeFloat64(5))
	conn.SubPut([]byte("yourfriends"), []byte("bob"), setop.EncodeFloat64(6))
	conn.SubPut([]byte("yourfriends"), []byte("charlie"), setop.EncodeFloat64(4))
	fmt.Printf("name score\n")
	for _, friend := range conn.SetExpression(setop.SetExpression{
		Code: "(U:FloatSum myfriends yourfriends)",
	}) {
		fmt.Printf("%v %v\n", string(friend.Key), fmt.Sprint(setop.DecodeFloat64(friend.Values[0])))
	}
	// Output:
	// name score
	// alice 10 <nil>
	// bob 11 <nil>
	// charlie 4 <nil>
}

func ExampleTreeMirror() {
	server := dhash.NewNodeDir("127.0.0.1:3030", "127.0.0.1:3030", "").MustStart()
	defer server.Stop()
	conn := client.MustConn("127.0.0.1:3030")
	conn.SubAddConfiguration([]byte("myfriends"), "mirrored", "yes")
	conn.SubPut([]byte("myfriends"), []byte("alice"), setop.EncodeFloat64(10))
	conn.SubPut([]byte("myfriends"), []byte("bob"), setop.EncodeFloat64(5))
	conn.SubPut([]byte("myfriends"), []byte("charlie"), setop.EncodeFloat64(6))
	conn.SubPut([]byte("myfriends"), []byte("denise"), setop.EncodeFloat64(4))
	fmt.Printf("name score\n")
	for _, friend := range conn.MirrorReverseSlice([]byte("myfriends"), nil, nil, true, true) {
		fmt.Printf("%v %v\n", fmt.Sprint(setop.DecodeFloat64(friend.Key)), string(friend.Value))
	}
	// Output:
	// name score
	// 10 <nil> alice
	// 6 <nil> charlie
	// 5 <nil> bob
	// 4 <nil> denise
}

package main

import (
	"flag"
	"fmt"
	"github.com/zond/god/common"
	"github.com/zond/god/dhash"
	"time"
)

var ip = flag.String("ip", "127.0.0.1", "IP address to listen to.")
var port = flag.Int("port", 9191, "Port to listen to for net/rpc connections. The next port will be used for the HTTP service.")
var joinIp = flag.String("joinIp", "", "IP address to join.")
var joinPort = flag.Int("joinPort", 9191, "Port to join.")

func main() {
	flag.Parse()
	s := dhash.NewNode(fmt.Sprintf("%v:%v", *ip, *port))
	s.AddChangeListener(func(ring *common.Ring) bool {
		fmt.Println(s.Describe())
		return true
	})
	s.AddMigrateListener(func(dhash *dhash.Node, source, destination []byte) bool {
		fmt.Printf("Migrated from %v to %v\n", common.HexEncode(source), common.HexEncode(destination))
		return true
	})
	s.AddSyncListener(func(source, dest common.Remote, pulled, pushed int) bool {
		fmt.Printf("%v pulled %v and pushed %v keys synchronizing with %v\n", source.Addr, pulled, pushed, dest.Addr)
		return true
	})
	s.AddCleanListener(func(source, dest common.Remote, cleaned, pushed int) bool {
		fmt.Printf("%v cleaned %v and pushed %v keys to %v\n", source.Addr, cleaned, pushed, dest.Addr)
		return true
	})
	s.MustStart()
	if *joinIp != "" {
		s.MustJoin(fmt.Sprintf("%v:%v", *joinIp, *joinPort))
	}

	for {
		time.Sleep(time.Second * 10)
	}
}

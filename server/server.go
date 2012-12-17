package main

import (
	"../common"
	"../dhash"
	"flag"
	"fmt"
	"time"
)

var ip = flag.String("ip", "127.0.0.1", "IP address to listen to")
var port = flag.Int("port", 9191, "Port to listen to")
var httpPort = flag.Int("httpPort", 8091, "Port to listen to for HTTP requests")
var joinIp = flag.String("joinIp", "", "IP address to join")
var joinPort = flag.Int("joinPort", 9191, "Port to join")

func main() {
	flag.Parse()
	s := dhash.NewNode(fmt.Sprintf("%v:%v", *ip, *port), *httpPort)
	s.AddChangeListener(func(ring *common.Ring) {
		fmt.Println(s.Describe())
	})
	s.AddMigrateListener(func(dhash *dhash.Node, source, destination []byte) {
		fmt.Printf("Migrated from %v to %v\n", common.HexEncode(source), common.HexEncode(destination))
	})
	s.AddSyncListener(func(dhash *dhash.Node, fetched, distributed int) {
		fmt.Printf("Fetched %v keys while distributing %v keys\n", fetched, distributed)
	})
	s.AddCleanListener(func(dhash *dhash.Node, cleaned, redistributed int) {
		fmt.Printf("Redistributed %v keys while cleaning %v keys\n", redistributed, cleaned)
	})
	s.MustStart()
	if *joinIp != "" {
		s.MustJoin(fmt.Sprintf("%v:%v", *joinIp, *joinPort))
	}

	for {
		time.Sleep(time.Second * 10)
	}
}

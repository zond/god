package main

import (
	"../shard"
	"fmt"
	"time"
)

func main() {
	s := shard.NewNode("127.0.0.1:9191")
	s2 := shard.NewNode("127.0.0.1:9192")
	s3 := shard.NewNode("127.0.0.1:9193")
	s.MustStart()
	s2.MustStart()
	s3.MustStart()
	s2.MustJoin("127.0.0.1:9191")
	s3.MustJoin("127.0.0.1:9191")
	fmt.Println(s)
	fmt.Println(s2)
	fmt.Println(s3)
	time.Sleep(time.Second)
	s.Stop()
	s2.Stop()
	s3.Stop()
}

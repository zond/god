package main

import (
	"../shard"
	"time"
)

func main() {
	s := shard.NewShard("127.0.0.1:9191")
	s2 := shard.NewShard("127.0.0.1:9192")
	s.MustStart()
	s2.MustStart()
	s2.Join("127.0.0.1:9192")
	time.Sleep(time.Second)
	s.Stop()
	s2.Stop()
}

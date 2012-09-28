package main

import (
	"../shard"
	"time"
)

func main() {
	s := shard.NewShard("239.255.4.5:7373")
	s2 := shard.NewShard("239.255.4.5:7373")
	s.Start()
	s2.Start()
	time.Sleep(time.Second)
	s.Stop()
	s2.Stop()
}

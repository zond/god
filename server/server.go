package main

import (
	"../shard"
	"time"
)

func main() {
	if s, err := shard.NewShard("239.255.4.5:7373"); err != nil {
		panic(err)
	} else {
		s.Start()
		if s2, err := shard.NewShard("239.255.4.5:7373"); err != nil {
			panic(err)
		} else {
			s2.Start()
			time.Sleep(time.Second * 10)
		}
	}
}

package main

import (
	"../dhash"
	"fmt"
	"time"
)

func main() {
	s := dhash.NewDHash("127.0.0.1:9191")
	s2 := dhash.NewDHash("127.0.0.1:9192")
	s3 := dhash.NewDHash("127.0.0.1:9193")
	s.MustStart()
	s2.MustStart()
	s3.MustStart()
	s2.MustJoin("127.0.0.1:9191")
	s3.MustJoin("127.0.0.1:9191")
	for {
		fmt.Println("***", time.Now())
		fmt.Println(s.Time())
		fmt.Println(s2.Time())
		fmt.Println(s3.Time())
		time.Sleep(time.Second)
	}
}

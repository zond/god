package main

import (
	"fmt"
	"os"
	"runtime/pprof"

	"github.com/zond/god/common"
	. "github.com/zond/god/dhash"
	"github.com/zond/god/murmur"
)

func main() {
	f, err := os.Create("cpuprofile")
	if err != nil {
		panic(err.Error())
	}
	f2, err := os.Create("memprofile")
	if err != nil {
		panic(err.Error())
	}

	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()
	defer pprof.WriteHeapProfile(f2)

	benchNode := NewNodeDir("127.0.0.1:1231", "127.0.0.1:1231", "")
	benchNode.MustStart()
	var k []byte
	for i := 0; i < 100000; i++ {
		k = murmur.HashString(fmt.Sprint(i))
		benchNode.Put(common.Item{
			Key:   k,
			Value: k,
		})
	}
}

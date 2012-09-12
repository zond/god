
package main

import (
	. "../"
	"runtime/pprof"
	"os"
	"../../murmur"
	"fmt"
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

	m := NewTree()
	var k []byte
	var v Hasher
	for i := 0; i < 1000000; i++ {
		k = murmur.HashString(fmt.Sprint(i))
		v = StringHasher(fmt.Sprint(i))
		m.Put(k, v)
		j, existed := m.Get(k)
		if j != v {
			panic(fmt.Errorf("%v should contain %v, but got %v, %v", m.Describe(), string(k), j, existed))
		}
	}
}

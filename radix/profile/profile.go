
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

	var k [][]byte
	var v []Hasher
	n := 100000
	for i := 0; i < n; i++ {
		k = append(k, murmur.HashString(fmt.Sprint(i)))
		v = append(v, StringHasher(fmt.Sprint(i)))
	}

	m := NewTree()
	for i := 0; i < n; i++ {
		m.Put(k[i], v[i])
		j, existed := m.Get(k[i])
		if j != v[i] {
			panic(fmt.Errorf("%v should contain %v, but got %v, %v", m.Describe(), string(k[i]), j, existed))
		}
	}
}

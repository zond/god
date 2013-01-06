package bench

import (
	"flag"
	"fmt"
	"net/rpc"
)

func RunMaster() {
	var ip = flag.String("ip", "127.0.0.1", "IP address to find a node at")
	var port = flag.Int("port", 9191, "Port to find a node at")
	var maxKey = flag.Int64("maxKey", 1000000, "Biggest key as int64 converted to []byte using common.EncodeInt64")
	flag.Parse()
	clients := make([]*rpc.Client, len(flag.Args()))
	rps := make([]float64, len(flag.Args()))
	var err error
	for index, addr := range flag.Args() {
		if clients[index], err = rpc.Dial("tcp", addr); err != nil {
			panic(err)
		}
	}
	command := SpinCommand{
		Addr:   fmt.Sprintf("%v:%v", *ip, *port),
		MaxKey: *maxKey,
	}
	for _, client := range clients {
		if err = client.Call("Slave.Spin", command, &Nothing{}); err == nil {
			fmt.Println(client, "is alive")
		} else {
			panic(err)
		}
	}
	for index, client := range clients {
		if err = client.Call("Slave.Wait", Nothing{}, &(rps[index])); err == nil {
			fmt.Println(client, "peaked")
		} else {
			panic(err)
		}
	}
	sum := float64(0)
	for _, r := range rps {
		sum += r
	}
	fmt.Println("Sum:", sum)
}

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
	var prepare = flag.Bool("prepare", false, "Whether to make sure that all keys within the maxKey range are set before starting")
	flag.Parse()
	clients := make([]*rpc.Client, len(flag.Args()))
	rps := make([]float64, len(flag.Args()))
	var err error
	for index, addr := range flag.Args() {
		if clients[index], err = rpc.Dial("tcp", addr); err != nil {
			panic(err)
		}
	}
	if *prepare {
		calls := make([]*rpc.Call, len(clients))
		for index, client := range clients {
			calls[index] = client.Go("Slave.Prepare", [2]int64{int64(index) * (*maxKey / int64(len(clients))), (int64(index) + 1) * (*maxKey / int64(len(clients)))}, &Nothing{}, nil)
		}
		for _, call := range calls {
			<-call.Done
			if call.Error != nil {
				panic(call.Error)
			}
		}
	}
	command := SpinCommand{
		Addr:   fmt.Sprintf("%v:%v", *ip, *port),
		MaxKey: *maxKey,
	}
	for index, client := range clients {
		if err = client.Call("Slave.Spin", command, &Nothing{}); err == nil {
			fmt.Println(flag.Args()[index], "is alive")
		} else {
			panic(err)
		}
	}
	for index, client := range clients {
		if err = client.Call("Slave.Wait", Nothing{}, &(rps[index])); err == nil {
			fmt.Println(flag.Args()[index], "peaked")
		} else {
			panic(err)
		}
	}
	sum := float64(0)
	for _, r := range rps {
		sum += r
	}
	fmt.Println("Peaked at", sum, "rps")
}

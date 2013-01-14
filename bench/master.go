package bench

import (
  "flag"
  "fmt"
  "net/rpc"
)

func RunMaster() {
  var ip = flag.String("ip", "127.0.0.1", "IP address to find a node at")
  var port = flag.Int("port", 9191, "Port to find a node at")
  var maxKey = flag.Int64("maxKey", 10000, "Biggest key as int64 converted to []byte using common.EncodeInt64")
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
  command := SpinCommand{
    Addr:   fmt.Sprintf("%v:%v", *ip, *port),
    MaxKey: *maxKey,
  }
  if *prepare {
    calls := make([]*rpc.Call, len(clients))
    for index, client := range clients {
      calls[index] = client.Go("Slave.Prepare", PrepareCommand{
        Addr:  command.Addr,
        Range: [2]int64{int64(index) * (*maxKey / int64(len(clients))), (int64(index) + 1) * (*maxKey / int64(len(clients)))},
      }, &Nothing{}, nil)
    }
    for _, call := range calls {
      <-call.Done
      if call.Error != nil {
        panic(call.Error)
      }
    }
  }
  var oldSpinRes *SpinResult
  var spinRes SpinResult
  for _, client := range clients {
    if err = client.Call("Slave.Spin", command, &spinRes); err != nil {
      panic(err)
    }
    if oldSpinRes == nil {
      oldSpinRes = &spinRes
    } else {
      if spinRes.Keys != oldSpinRes.Keys || spinRes.Nodes != oldSpinRes.Nodes {
        panic(fmt.Errorf("Last slave had %v nodes and %v keys, now I get %v nodes and %v keys?", oldSpinRes.Nodes, oldSpinRes.Keys, spinRes.Nodes, spinRes.Keys))
      }
    }
  }
  for index, client := range clients {
    if err = client.Call("Slave.Wait", Nothing{}, &(rps[index])); err != nil {
      panic(err)
    }
  }
  sum := float64(0)
  for _, r := range rps {
    sum += r
  }
  fmt.Printf("%v\t%v\t%v\n", spinRes.Nodes, spinRes.Keys, sum)
}

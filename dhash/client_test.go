package dhash

import (
	"../common"
	"fmt"
	"testing"
	"time"
)

func startServers() (result []*DHash) {
	for i := 0; i < common.Redundancy*2; i++ {
		result = append(result, NewDHash(fmt.Sprintf("127.0.0.1:%v", 9200+i)))
	}
	for _, d := range result {
		d.MustStart()
	}
	for i := 1; i < len(result); i++ {
		result[i].MustJoin(result[0].GetAddr())
	}
	return
}

func TestClient(t *testing.T) {
	fmt.Println("starting up client_test")
	startServers()
	fmt.Println("waiting")
	time.Sleep(time.Second * 5)
	fmt.Println("client_test done")
}

package shard

import (
	"fmt"
	"log"
	"net"
)

type Shard struct {
	clusterAddress    *net.UDPAddr
	clusterConnection *net.UDPConn
}

func NewShard(address string) (result *Shard, err error) {
	result = &Shard{}
	result.clusterAddress, err = net.ResolveUDPAddr("udp", address)
	return
}
func (self *Shard) listenMulticast() {
	if connection, err := net.ListenMulticastUDP("udp", nil, self.clusterAddress); err != nil {
		panic(fmt.Errorf("While trying to open %v: %v", self.clusterAddress, err))
	} else {
		self.clusterConnection = connection
		log.Printf("Started listening to %v", self.clusterConnection)
		message := make([]byte, 512)
		var err error
		for err == nil {
			if _, err = self.clusterConnection.Read(message); err == nil {
				log.Printf("Got %v", message)
			}
		}
		log.Printf("Stopped listening to %v: %v", self.clusterConnection, err)
	}
}
func (self *Shard) Stop() {
	self.clusterConnection.Close()
}
func (self *Shard) multicast(message []byte) {
	if conn, err := net.DialUDP("udp", nil, self.clusterAddress); err != nil {
		panic(fmt.Errorf("While trying to connect to %v: %v", self.clusterAddress, err))
	} else {
		if sent, err := conn.Write(message); err != nil {
			panic(fmt.Errorf("While trying to send %v to %v: %v", message, self.clusterAddress, err))
		} else if sent != len(message) {
			panic(fmt.Errorf("While trying to send %v to %v, only sent %v/%v bytes", message, self.clusterAddress, sent, len(message)))
		}
	}
}
func (self *Shard) Start() {
	go self.listenMulticast()
	self.multicast([]byte("hehu!"))
}

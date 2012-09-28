package shard

import (
	"fmt"
	"log"
	"net"
)

const (
	join = iota
)

type messageType byte

func (self messageType) String() string {
	switch self {
	case join:
		return "join"
	}
	return fmt.Sprintf("Unknown messageType: %#v", self)
}

type udpMessage struct {
	message messageType
	source  *net.TCPAddr
}

func (self udpMessage) String() string {
	return fmt.Sprintf("%v %v", self.message, self.source)
}
func (self udpMessage) encode() (result []byte) {
	result = make([]byte, 1+16+4)
	result[0] = byte(self.message)
	copy(result[1:], self.source.IP)
	result[len(result)-4] = byte(self.source.Port >> 24)
	result[len(result)-3] = byte(self.source.Port >> 16)
	result[len(result)-2] = byte(self.source.Port >> 8)
	result[len(result)-1] = byte(self.source.Port)
	return result
}
func decodeUdpMessage(message []byte) (result udpMessage) {
	result.message = messageType(message[0])
	result.source = &net.TCPAddr{make([]byte, 16), 0}
	copy(result.source.IP, message[1:16+1])
	result.source.Port += int(message[16+1]) << 24
	result.source.Port += int(message[16+2]) << 16
	result.source.Port += int(message[16+3]) << 8
	result.source.Port += int(message[16+4])
	return result
}

type Shard struct {
	clusterAddressString string
	clusterAddress       *net.UDPAddr
	clusterConnection    *net.UDPConn
	addressString        string
	address              *net.TCPAddr
}

func NewShard(address string) *Shard {
	return &Shard{
		clusterAddressString: address,
	}
}
func (self *Shard) String() string {
	return fmt.Sprintf("%v@%v", self.addressString, self.clusterAddressString)
}
func (self *Shard) Verify() (err error) {
	if self.clusterAddress == nil && self.address == nil {
		if self.clusterAddress, err = net.ResolveUDPAddr("udp", self.clusterAddressString); err != nil {
			return
		}
		if self.addressString == "" {
			var tmpConn *net.UDPConn
			if tmpConn, err = net.DialUDP("udp", nil, self.clusterAddress); err != nil {
				return
			}
			self.addressString = tmpConn.LocalAddr().String()
			tmpConn.Close()
		}
		self.address, err = net.ResolveTCPAddr("tcp", self.addressString)
	}
	return
}
func (self *Shard) listenMulticast() {
	if connection, err := net.ListenMulticastUDP("udp", nil, self.clusterAddress); err != nil {
		panic(fmt.Errorf("While trying to open %v: %v", self.clusterAddress, err))
	} else {
		self.clusterConnection = connection
		log.Printf("Started listening to %v", self.clusterAddress)
		message := make([]byte, 512)
		var err error
		for err == nil {
			if _, err = self.clusterConnection.Read(message); err == nil {
				log.Printf("Got %v", decodeUdpMessage(message))
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
func (self *Shard) createUdpMessage(message messageType) udpMessage {
	return udpMessage{message, self.address}
}
func (self *Shard) Start() (err error) {
	if err = self.Verify(); err != nil {
		return
	}
	go self.listenMulticast()
	self.multicast(self.createUdpMessage(join).encode())
	return
}

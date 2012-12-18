package dhash

import (
	"../common"
	"../web"
	"bytes"
	"code.google.com/p/go.net/websocket"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"net"
	"net/http"
	"net/rpc"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type socketMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

var prefPattern = regexp.MustCompile("^([^\\s;]+)(;q=([\\d.]+))?$")

func mostAccepted(r *http.Request, def, name string) string {
	bestValue := def
	var bestScore float64 = -1
	var score float64
	for _, pref := range strings.Split(r.Header.Get(name), ",") {
		if match := prefPattern.FindStringSubmatch(pref); match != nil {
			score = 1
			if match[3] != "" {
				score = common.MustParseFloat64(match[3])
			}
			if score > bestScore {
				bestScore = score
				bestValue = match[1]
			}
		}
	}
	return bestValue
}

func wantsJSON(r *http.Request, m *mux.RouteMatch) bool {
	return mostAccepted(r, "text/html", "Accept") == "application/json"
}

func wantsHTML(r *http.Request, m *mux.RouteMatch) bool {
	return mostAccepted(r, "text/html", "Accept") == "text/html"
}

type requestContext struct {
	method   string
	request  *http.Request
	response http.ResponseWriter
}

func (self *requestContext) ReadRequestHeader(r *rpc.Request) error {
	*r = rpc.Request{
		ServiceMethod: self.method,
	}
	return nil
}

func (self *requestContext) getBodyString() string {
	b := make([]byte, self.request.ContentLength)
	if _, err := io.ReadFull(self.request.Body, b); err != nil {
		panic(err)
	}
	return string(b)
}

func (self *requestContext) ReadRequestBody(b interface{}) (err error) {
	if b != nil {
		if _, ok := b.(*int); ok {
			var i int64
			if i, err = strconv.ParseInt(self.getBodyString(), 10, 64); err != nil {
				return
			}
			reflect.ValueOf(b).Elem().SetInt(i)
		} else {
			err = json.NewDecoder(self.request.Body).Decode(b)
		}
	}
	return
}

func (self *requestContext) WriteResponse(resp *rpc.Response, b interface{}) (err error) {
	self.response.Header().Set("Content-Type", "application/json; charset=UTF-8")
	buffer := new(bytes.Buffer)
	if resp.Error != "" {
		self.response.WriteHeader(500)
		if err = json.NewEncoder(buffer).Encode(resp.Error); err != nil {
			return
		}
	} else {
		if err = json.NewEncoder(buffer).Encode(b); err != nil {
			return
		}
	}
	self.response.Header().Set("Content-Length", fmt.Sprint(buffer.Len()))
	_, err = self.response.Write(buffer.Bytes())
	return
}

func (self *requestContext) Close() error {
	return self.request.Body.Close()
}

type jsonRpcServer struct {
	server *rpc.Server
}

func (self jsonRpcServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	context := &requestContext{
		method:   mux.Vars(r)["method"],
		request:  r,
		response: w,
	}
	self.server.ServeRequest(context)
}

func (self *Node) startJson() {
	var nodeAddr *net.TCPAddr
	var err error
	if nodeAddr, err = net.ResolveTCPAddr("tcp", self.node.GetAddr()); err != nil {
		return
	}
	rpcServer := rpc.NewServer()
	rpcServer.RegisterName("DHash", (*jsonDhashServer)(self))
	jsonServer := jsonRpcServer{server: rpcServer}
	router := mux.NewRouter()
	router.Methods("POST").Path("/rpc/{method}").MatcherFunc(wantsJSON).Handler(jsonServer)
	web.Route(func(ws *websocket.Conn) {
		b, err := json.Marshal(socketMessage{
			Type: "RingChange",
			Data: map[string]interface{}{
				"description": self.Description(),
				"routes":      self.node.Nodes(),
			},
		})
		if err != nil {
			panic(err)
		}
		if websocket.Message.Send(ws, string(b)) == nil {
			self.AddChangeListener(func(ring *common.Ring) bool {
				b, err := json.Marshal(socketMessage{
					Type: "RingChange",
					Data: map[string]interface{}{
						"description": self.Description(),
						"routes":      self.node.Nodes(),
					},
				})
				if err != nil {
					panic(err)
				}
				return websocket.Message.Send(ws, string(b)) == nil
			})
			self.AddMigrateListener(func(dhash *Node, source, destination []byte) bool {
				b, err := json.Marshal(socketMessage{
					Type: "Migration",
					Data: [][]byte{source, destination},
				})
				if err != nil {
					panic(err)
				}
				return websocket.Message.Send(ws, string(b)) == nil
			})
			self.AddSyncListener(func(dhash *Node, fetched, distributed int) bool {
				b, err := json.Marshal(socketMessage{
					Type: "Sync",
					Data: []int{fetched, distributed},
				})
				if err != nil {
					panic(err)
				}
				return websocket.Message.Send(ws, string(b)) == nil
			})
			self.AddCleanListener(func(dhash *Node, cleaned, redistributed int) bool {
				b, err := json.Marshal(socketMessage{
					Type: "Clean",
					Data: []int{cleaned, redistributed},
				})
				if err != nil {
					panic(err)
				}
				return websocket.Message.Send(ws, string(b)) == nil
			})
			var mess string
			for {
				if err = websocket.Message.Receive(ws, &mess); err != nil {
					break
				}
			}
		}
	}, router)
	http.Handle("/", router)
	go http.ListenAndServe(fmt.Sprintf("%v:%v", nodeAddr.IP, self.getHTTPPort()), router)
}

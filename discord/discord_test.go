
package discord


import (
	"testing"
	"time"
	"fmt"
)

func assertWithin(t *testing.T, f func() (string, bool), d time.Duration) {
	deadline := time.Now().Add(d)
	var ok bool
	var msg string
	for time.Now().Before(deadline) {
		if msg, ok = f(); ok {
			return
		}
		time.Sleep(time.Second)
	}
	t.Errorf("wanted %v to be true within %v, but it never happened: %v", f, d, msg)
}

func TestStartup(t *testing.T) {
	firstPort := 9191
	var nodes []*Node
	n := 10
	for i := 0; i < n; i++ {
		nodes = append(nodes, NewNode(fmt.Sprintf("%v:%v", "127.0.0.1", firstPort + i)))
	}
	for i := 0; i < n; i++ {
		nodes[i].MustStart()
	}
	for i := 1; i < n; i++ {
		nodes[i].MustJoin(nodes[0].GetAddr())
	}
	assertWithin(t, func() (string, bool) {
		routes := make(map[string]bool)
		for i := 0; i < n; i++ {
			routes[nodes[i].Nodes().Describe()] = true
		}
		return fmt.Sprint(routes), len(routes) == 1
	}, time.Second * 10)
}
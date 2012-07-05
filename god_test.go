
package god

import (
	"testing"
	"fmt"
)

func openGod(p string) *God {
	g, err := NewGod(p)
	if err == nil {
		return g
	}
	panic(fmt.Errorf("should be able to open %v, got %v", p, err))
}

func TestBlaj(t *testing.T) {
	g := openGod("test1")
	resp := Response{}
	g.Perform(Operation{KEYS, nil}, &resp)
	fmt.Println(resp)
	g.Perform(Operation{PUT, []string{"k", "v"}}, &resp)
	fmt.Println(resp)
	g.Perform(Operation{GET, []string{"k"}}, &resp)
	fmt.Println(resp)
}

package god

import (
	"testing"
	"fmt"
	"os"
	"github.com/simonz05/godis"
)

func openGod(p string) *God {
	g, err := NewGod(p)
	if err == nil {
		return g
	}
	panic(fmt.Errorf("should be able to open %v, got %v", p, err))
}

func BenchmarkPut(b *testing.B) {
	os.RemoveAll("bench1")
	g := openGod("bench1")
	defer g.Close()
	resp := Response{}
	oper := Operation{PUT, make([]string, 2)}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		oper.Parameters[0] = fmt.Sprint(i)
		oper.Parameters[1] = oper.Parameters[0]
		g.Perform(oper, &resp)
	}
	b.StopTimer()
}

func BenchmarkGodis(b *testing.B) {
	c := godis.New("", 0, "")
	var p string 
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		p = fmt.Sprint(i)
		c.Set(p, p)
	}
	b.StopTimer()
}

func TestBlaj(t *testing.T) {
	g := openGod("test1")
	defer g.Close()
	resp := Response{}
	g.Perform(Operation{CLEAR, nil}, &resp)
	g.Perform(Operation{PUT, []string{"k", "v"}}, &resp)
	g.Perform(Operation{GET, []string{"k"}}, &resp)
	if !resp.Ok() || resp.Parts[0] != "v" {
		t.Error("should get 'v' but got", resp)
	}
}
package main

import (
	"fmt"
	"github.com/zond/god/client"
	"github.com/zond/god/common"
	"github.com/zond/setop"
	"math/rand"
)

var conn = client.MustConn("localhost:9191")

type user string

func (self user) like(flavour string) {
	// remember that we have liked this flavour by putting it in our sub tree with nil value, and in the sub tree of the flavour with value 1
	conn.SubPut([]byte(self), []byte(flavour), nil)
	conn.SubPut([]byte(flavour), []byte(self), common.EncodeInt64(1))
}

func (self user) similar() user {
	// create a set operation that returns the union of the tasters of all flavors we have rated, summing the values
	op := &setop.SetOp{
		Merge: setop.IntegerSum,
		Type:  setop.Union,
	}
	// for each flavor we have tried, add the raters of that flavor as a source
	for _, flavour := range conn.Slice([]byte(self), nil, nil, true, true) {
		op.Sources = append(op.Sources, setop.SetOpSource{Key: flavour.Key})
	}
	// designate a dump subset
	dumpkey := []byte(fmt.Sprintf("%v_similar_%v", self, rand.Int63()))
	// make it mirrored
	conn.SubAddConfiguration(dumpkey, "mirrored", "yes")
	// make sure we clean up after ourselves
	defer conn.SubClear(dumpkey)
	// run the set operation dumping the values in the dump tree
	conn.SetExpression(setop.SetExpression{
		Op:   op,
		Dest: dumpkey,
	})
	for _, user := range conn.Slice(dumpkey, nil, nil, true, true) {
		fmt.Printf("%v has liked %v flavours in common with %v\n", string(user.Key), common.MustDecodeInt64(user.Value), self)
	}
	// fetch the second best match (the best is likely us...)
	best := conn.MirrorReverseSliceLen(dumpkey, nil, true, 2)[1]
	return user(best.Value)
}

func main() {
	adam := user("adam")
	beatrice := user("beatrice")
	charlie := user("charlie")
	denise := user("denise")
	eddard := user("eddard")
	adam.like("vanilla")
	adam.like("licorice")
	adam.like("mulberry")
	adam.like("wood shavings")
	beatrice.like("licorice")
	beatrice.like("chocolate")
	charlie.like("strawberry")
	charlie.like("pumpkin")
	denise.like("vanilla")
	denise.like("mulberry")
	denise.like("strawberry")
	eddard.like("steel")
	eddard.like("snow")
	fmt.Println("adam should get to know", adam.similar())
}

// output: adam has liked 4 flavours in common with adam
// output: beatrice has liked 1 flavours in common with adam
// output: charlie has liked 1 flavours in common with adam
// output: denise has liked 3 flavours in common with adam
// output: adam should get to know denise

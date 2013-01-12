package main

import (
  "fmt"
  "github.com/zond/god/client"
  "github.com/zond/god/common"
  "github.com/zond/god/setop"
  "math"
  "math/rand"
)

func abs(i int64) int64 {
  if i < 0 {
    return -i
  }
  return i
}

var conn = client.MustConn("localhost:9191")

type recommendation struct {
  name  flavour
  score float64
}

type user string

func (self user) rate(f flavour, score int) {
  // store our rating of this flavour under our key
  conn.SubPut([]byte(self), []byte(f), common.EncodeInt64(int64(score)))
  // store that we have rated this flavour under the flavour key
  conn.SubPut([]byte(f), []byte(self), nil)
}
func (self user) recommended() recommendation {
  // Create a set operation that returns the union of the tasters of all flavors we have rated, just returning the taster key
  op := &setop.SetOp{
    Merge: setop.First,
    Type:  setop.Union,
  }
  // for each flavor we have tried, add the raters of that flavor as a source
  for _, flavour := range conn.Slice([]byte(self), nil, nil, true, true) {
    op.Sources = append(op.Sources, setop.SetOpSource{Key: flavour.Key})
  }
  // designate a dump subset
  dumpkey := []byte(fmt.Sprintf("%v_recommended_%v", self, rand.Int63()))
  // make it mirrored
  conn.SubAddConfiguration(dumpkey, "mirrored", "yes")
  // make sure we clean up after ourselves
  defer conn.SubClear(dumpkey)
  // execute the set expression, and for each rater
  for _, u := range conn.SetExpression(setop.SetExpression{
    Op: op,
  }) {
    // if the rater is not us
    if user(u.Key) != self {
      var sum int64
      var count int
      // fetch the intersection of the flavours we and the other taster has tried, subtracting the ratings from each other, and for each match
      for _, f := range conn.SetExpression(setop.SetExpression{
        Code: fmt.Sprintf("(I:IntegerSub %v %v)", self, string(u.Key)),
      }) {
        // sum the similarity between our ratings
        sum += (10 - abs(common.MustDecodeInt64(f.Values[0])))
        count++
      }
      fmt.Printf("%v has %v ratings in common with %v, and they are %v similar\n", string(u.Key), count, self, sum)
      avg_similarity := float64(sum) / float64(count)
      // let the relevance of this user be the average similarity times log(count of common ratings + 1)
      weight := avg_similarity * math.Log(float64(count+1))
      fmt.Printf("this gives them an average similarity of %v, and a weight of %v\n", avg_similarity, weight)
      // fetch the difference between the ratings of the other rater and us (ie the flavours the other rater has tried, but we havent), 
      // just returning the first value
      for _, f := range conn.SetExpression(setop.SetExpression{
        Code: fmt.Sprintf("(D:First %v %v)", string(u.Key), self),
      }) {
        // recommendation modification for this untried flavour
        modification := weight * float64(common.MustDecodeInt64(f.Values[0]))
        fmt.Printf("%v has tried %v, which %v has not, and rated it %v, which gives it a recommendation value influence of %v", string(u.Key), string(f.Key), self, common.MustDecodeInt64(f.Values[0]), modification)
        // sum this modification in the dump subset
        if previous, existed := conn.SubGet(dumpkey, f.Key); existed {
          sum := common.MustDecodeFloat64(previous) + modification
          conn.SubPut(dumpkey, f.Key, common.EncodeFloat64(sum))
          fmt.Printf(", adding up to %v\n", sum)
        } else {
          conn.SubPut(dumpkey, f.Key, common.EncodeFloat64(modification))
          fmt.Printf("\n")
        }
      }
    }
  }
  // return the highest rated recommendation
  best := conn.SliceLen(dumpkey, nil, true, 1)[0]
  return recommendation{name: flavour(best.Key), score: common.MustDecodeFloat64(best.Value)}
}

type flavour string

func main() {
  adam := user("adam")
  beatrice := user("beatrice")
  charlie := user("charlie")
  denise := user("denise")
  eddard := user("eddard")
  adam.rate("vanilla", 4)
  adam.rate("strawberry", 1)
  adam.rate("licorice", 10)
  beatrice.rate("vanilla", 2)
  beatrice.rate("licorice", 7)
  beatrice.rate("chocolate", 4)
  charlie.rate("strawberry", 6)
  charlie.rate("chocolate", 3)
  charlie.rate("pumpkin", 10)
  denise.rate("vanilla", 10)
  denise.rate("strawberry", 10)
  denise.rate("licorice", 1)
  eddard.rate("blood", 0)
  eddard.rate("steel", 5)
  eddard.rate("snow", 8)
  fmt.Println("with the data we have, we recommend that adam tries", adam.recommended().name)
}

// output: beatrice has 2 ratings in common with adam, and they are 15 similar
// output: this gives them an average similarity of 7.5, and a weight of 8.239592165010821
// output: beatrice has tried chocolate, which adam has not, and rated it 4, which gives it a recommendation value influence of 32.958368660043284
// output: charlie has 1 ratings in common with adam, and they are 5 similar
// output: this gives them an average similarity of 5, and a weight of 3.4657359027997265
// output: charlie has tried chocolate, which adam has not, and rated it 3, which gives it a recommendation value influence of 10.39720770839918, adding up to 43.35557636844246
// output: charlie has tried pumpkin, which adam has not, and rated it 10, which gives it a recommendation value influence of 34.657359027997266
// output: denise has 3 ratings in common with adam, and they are 6 similar
// output: this gives them an average similarity of 2, and a weight of 2.772588722239781
// output: with the data we have, we recommend that adam tries chocolate

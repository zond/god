package dhash

import (
	"../common"
	"time"
)

type remotePeer common.Remote

func (self remotePeer) ActualTime() (result time.Time) {
	if err := (common.Remote)(self).Call("Timer.ActualTime", 0, &result); err != nil {
		result = time.Now()
	}
	return
}

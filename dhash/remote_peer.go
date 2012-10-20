package dhash

import (
	"../common"
	"time"
)

type remotePeer common.Remote

func (self remotePeer) ActualTime() (result int64) {
	if err := (common.Remote)(self).Call("Timer.ActualTime", 0, &result); err != nil {
		result = time.Now().UnixNano()
	}
	return
}

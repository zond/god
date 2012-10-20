package dhash

import (
	"../timenet"
)

type timerServer timenet.Timer

func (self *timerServer) ActualTime(x int, result *int64) error {
	*result = (*timenet.Timer)(self).ActualTime()
	return nil
}

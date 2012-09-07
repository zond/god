
package shard

import (
	"strconv"
)

type logNames []string
func (self logNames) Len() int {
	return len(self)
}
func (self logNames) Less(i, j int) bool {
	vi := uint64(0)
	vj := uint64(0)
	if logPattern.MatchString(self[i]) {
		vi, _ = strconv.ParseUint(logPattern.FindStringSubmatch(self[i])[1], 10, 64)
	}
	if logPattern.MatchString(self[j]) {
		vj, _ = strconv.ParseUint(logPattern.FindStringSubmatch(self[j])[1], 10, 64)
	}
	return vi < vj
}
func (self logNames) Swap(i, j int) {
	self[i], self[j] = self[j], self[i]
}


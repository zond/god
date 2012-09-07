
package shard

import (
	"os"
	"encoding/gob"
	"fmt"
	"runtime"
)

type fileDecoder struct {
	*gob.Decoder
	file *os.File
}
func newFileDecoder(path string) *fileDecoder {
	file, err := os.Open(path)
	if err != nil {
		panic(fmt.Errorf("While trying to open %v: %v", path, err))
	}
	rval := &fileDecoder{gob.NewDecoder(file), file} 
	runtime.SetFinalizer(rval, func(decoder *fileDecoder) {
		decoder.file.Close()
	})
	return rval
}

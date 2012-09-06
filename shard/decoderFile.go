
package shard

import (
	"os"
	"encoding/gob"
	"fmt"
	"runtime"
)

type decoderFile struct {
	*gob.Decoder
	file *os.File
}
func newDecoderFile(path string) *decoderFile {
	file, err := os.Open(path)
	if err != nil {
		panic(fmt.Errorf("While trying to open %v: %v", path, err))
	}
	rval := &decoderFile{gob.NewDecoder(file), file} 
	runtime.SetFinalizer(rval, func(decoder *decoderFile) {
		decoder.file.Close()
	})
	return rval
}

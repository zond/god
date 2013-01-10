package god

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
)

const (
	examples = "examples"
)

var goPattern = regexp.MustCompile("^.*\\.go$")
var jsPattern = regexp.MustCompile("^.*\\.js$")

func TestExamples(t *testing.T) {
	dir, err := os.Open(examples)
	if err != nil {
		panic(err)
	}
	files, err := dir.Readdirnames(-1)
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		var c *exec.Cmd
		if goPattern.MatchString(file) {
			c = exec.Command("go", "run", filepath.Join(examples, file))
		} else if jsPattern.MatchString(file) {
			c = exec.Command("node", filepath.Join(examples, file))
		}
		if c != nil {
			buf := new(bytes.Buffer)
			c.Stderr = buf
			if err := c.Run(); err != nil {
				t.Error("While trying to run", file, ": ", err)
				if buf.Len() > 0 {
					fmt.Println(string(buf.Bytes()))
				}
				os.Exit(1)
			}
			if buf.Len() > 0 {
				t.Error("While running ", file, ": ", string(buf.Bytes()))
				os.Exit(2)
			}
		}
	}
}

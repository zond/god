package god

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

const (
	examples = "examples"
)

var goPattern = regexp.MustCompile("^.*\\.go$")
var jsPattern = regexp.MustCompile("^.*\\.js$")
var outputPattern = regexp.MustCompile("^// output: (.*)$")

func getExpected(f string) string {
	r, err := os.Open(f)
	if err != nil {
		panic(err)
	}
	buf := bufio.NewReader(r)
	rval := new(bytes.Buffer)
	for line, err := buf.ReadString('\n'); err == nil; line, err = buf.ReadString('\n') {
		if match := outputPattern.FindStringSubmatch(strings.TrimSpace(line)); match != nil {
			fmt.Fprintln(rval, strings.TrimSpace(match[1]))
		}
	}
	return string(rval.Bytes())
}

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
		var expected string
		var c *exec.Cmd
		if goPattern.MatchString(file) {
			expected = getExpected(filepath.Join(examples, file))
			c = exec.Command("go", "run", filepath.Join(examples, file))
		} else if jsPattern.MatchString(file) {
			expected = getExpected(filepath.Join(examples, file))
			c = exec.Command("node", filepath.Join(examples, file))
		}
		if c != nil {
			stderr := new(bytes.Buffer)
			c.Stderr = stderr
			stdout := new(bytes.Buffer)
			c.Stdout = stdout
			if err := c.Run(); err != nil {
				t.Error("While trying to run", file, ": ", err)
				if stderr.Len() > 0 {
					fmt.Println(string(stderr.Bytes()))
				}
				os.Exit(1)
			}
			if stderr.Len() > 0 {
				t.Error("While running ", file, ": ", string(stderr.Bytes()))
				os.Exit(2)
			}
			bufout := bufio.NewReader(stdout)
			cleanout := new(bytes.Buffer)
			for line, err := bufout.ReadString('\n'); err == nil; line, err = bufout.ReadString('\n') {
				fmt.Fprintln(cleanout, strings.TrimSpace(line))
			}
			if string(cleanout.Bytes()) != expected {
				t.Errorf("%v: Expected \n%v\n but got \n%v\n", file, expected, string(cleanout.Bytes()))
			}
		}
	}
}

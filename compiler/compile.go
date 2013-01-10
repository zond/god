package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"text/template"
)

var templates = template.Must(template.New("templates").ParseGlob("templates/*.html"))

type examplefinder struct{}

func (self examplefinder) E(name string) string {
	var err error
	var f *os.File
	f, err = os.Open(filepath.Join("examples", name))
	if err != nil {
		panic(err)
	}
	buf := new(bytes.Buffer)
	if _, err = io.Copy(buf, f); err != nil {
		panic(err)
	}
	return string(buf.Bytes())
}

func main() {
	for _, template := range templates.Templates() {
		out, err := os.Create(template.Name())
		if err != nil {
			panic(err)
		}
		if err = template.Execute(out, examplefinder{}); err != nil {
			panic(err)
		}
		if err = out.Close(); err != nil {
			panic(err)
		}
	}
}

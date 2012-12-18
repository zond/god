package web

import (
	"../templates"
	"code.google.com/p/go.net/websocket"
	"fmt"
	"github.com/gorilla/mux"
	htmlTemplate "html/template"
	"net/http"
	textTemplate "text/template"
)

type baseData struct {
	Timestamp int64
}

func getBaseData(w http.ResponseWriter, r *http.Request) baseData {
	return baseData{
		Timestamp: templates.Timestamp,
	}
}
func (self baseData) T() string {
	return fmt.Sprint(self.Timestamp)
}

func allCss(w http.ResponseWriter, r *http.Request) {
	data := getBaseData(w, r)
	w.Header().Set("Content-Type", "text/css; charset=UTF-8")
	renderText(w, r, templates.CSS, "bootstrap.min.css", data)
	renderText(w, r, templates.CSS, "common.css", data)
}

func allJs(w http.ResponseWriter, r *http.Request) {
	data := getBaseData(w, r)
	w.Header().Set("Content-Type", "application/javascript; charset=UTF-8")
	renderText(w, r, templates.JS, "underscore-min.js", data)
	renderText(w, r, templates.JS, "jquery-1.8.3.min.js", data)
	renderText(w, r, templates.JS, "bootstrap.min.js", data)
	renderText(w, r, templates.JS, "jcanvas.min.js", data)
	renderText(w, r, templates.JS, "jquery.websocket-0.0.1.js", data)
	renderText(w, r, templates.JS, "big.min.js", data)
	renderText(w, r, templates.JS, "jquery.base64.js", data)
	renderText(w, r, templates.JS, "god.js", data)
}

func renderHtml(w http.ResponseWriter, r *http.Request, templates *htmlTemplate.Template, template string, data interface{}) {
	w.Header().Set("Content-Type", "text/html; charset=UTF-8")
	if err := templates.ExecuteTemplate(w, template, data); err != nil {
		panic(fmt.Errorf("While rendering HTML: %v", err))
	}
}

func renderText(w http.ResponseWriter, r *http.Request, templates *textTemplate.Template, template string, data interface{}) {
	if err := templates.ExecuteTemplate(w, template, data); err != nil {
		panic(fmt.Errorf("While rendering text: %v", err))
	}
}

func index(w http.ResponseWriter, r *http.Request) {
	renderHtml(w, r, templates.HTML, "index.html", getBaseData(w, r))
}

func Route(handler websocket.Handler, router *mux.Router) {
	router.HandleFunc("/js/{ver}/all.js", allJs)
	router.HandleFunc("/css/{ver}/all.css", allCss)
	router.Path("/ws").Handler(handler)
	router.HandleFunc("/", index)
}

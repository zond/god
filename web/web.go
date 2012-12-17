package web

import (
	"../templates"
	"fmt"
	"github.com/gorilla/mux"
	"net/http"
)

type baseData struct {
	Timestamp int64
}

func getBaseData(w http.ResponseWrite, r *http.Request) baseData {
	return baseData{
		Timestamp: templates.Timestamp,
	}
}
func (self baseData) T() string {
	return fmt.Sprint(self.Timestamp)
}

func allCss(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/css; charset=UTF-8")
	renderText(w, r, templates.CSS, "bootstrap.min.css", nil)
}

func allJs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/javascript; charset=UTF-8")
	renderText(w, r, templates.JS, "underscore-min.js", nil)
	renderText(w, r, templates.JS, "jquery-1.8.3.min.js", nil)
	renderText(w, r, templates.JS, "bootstrap.min.js", nil)
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
	renderHtml(w, r, templates.HTML, "index.html", nil)
}

func Route(router *mux.Router) {
	router.HandleFunc("/js/{ver}/all.js", allJs)
	router.HandleFunc("/css/{ver}/all.css", allCss)
	router.HandleFunc("/", index)
}

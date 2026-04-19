package web

import (
	"embed"
	"html/template"
	"io"
)

//go:embed templates/*.html
var templateFS embed.FS

var tpl = template.Must(template.New("").ParseFS(templateFS, "templates/*.html"))

func render(w io.Writer, name string, data any) error {
	return tpl.ExecuteTemplate(w, name, data)
}

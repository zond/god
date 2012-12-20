package templates
import "html/template"
var HTML = template.New("html")
func init() {
  template.Must(HTML.New("index.html").Parse("<html>\n	<head>\n		<title>\n			Go Database!\n		</title>\n		<link href=\"/css/{{.T}}/all.css\" rel=\"stylesheet\" media=\"screen\">\n		<script src=\"/js/{{.T}}/all.js\" type=\"text/javascript\"></script>\n	</head>\n	<body>		\n		<div id=\"chord_container\">\n			<canvas width=\"3000\" height=\"2000\" id=\"chord\"></canvas>\n		</div>\n		<div id=\"node_container\">\n			<dl>\n				<dt>gob RPC Address</dt>\n				<dd id=\"node_gob_addr\"></dd>\n				<dt>JSON/HTTP RPC Address</dt>\n				<dd id=\"node_json_addr\"></dd>\n				<dt>Position</dt>\n				<dd id=\"node_pos\"></dd>\n				<dt>Owned keys</dt>\n				<dd id=\"node_owned_keys\"></dd>\n				<dt>Held keys</dt>\n				<dd id=\"node_held_keys\"></dd>\n			</dl>\n		</div>\n	</body>\n</html>\n"))
}


Node = function(g, data) {
  var that = new Object();
	that.data = data;
	that.gob_addr = data.Addr;
	var m = /^(.*):(.*)$/.exec(that.gob_addr);
	that.json_addr = m[1] + ":" + (1 + parseInt(m[2]));
	that.angle = Big(3 * Math.PI / 2).minus($.base64.decode2big(data.Pos).div(g.maxPos).times(Math.PI * 2)).toFixed();
	that.hexpos = "";
	_.each($.base64.decode2b(data.Pos), function(b) {
	  var thishex = b.toString(16);
		while (thishex.length < 2) {
		  thishex = "0" + thishex;
		}
	  that.hexpos += thishex;
	});
	that.x = g.cx + Math.cos(that.angle) * g.r;
	that.y = g.cy + Math.sin(that.angle) * g.r;
	while (that.hexpos.length < 32) {
	  that.hexpos = "0" + that.hexpos;
	}
	return that;
}

God = function() {
  var that = new Object();
	that.maxPos = Big(1).times(Big(256).pow(16));
	that.cx = 1000;
	that.cy = 1000;
	that.r = 800;
  that.drawChord = function() {
	  $("#chord").clearCanvas().drawArc({
			layer: true,
		  strokeStyle: "black",
			x: that.cx,
			y: that.cy,
			radius: that.r,
		}).drawLine({
			layer: true,
		  strokeStyle: "black",
		  x1: that.cx, y1: 180,
			x2: that.cx, y2: 220,
		});
		_.each(that.routes, function(route) {
			$("#chord").drawArc({
				layer: true,
				fillStyle: "black",
				x: route.x,
				y: route.y,
				radius: 10,
			}).drawText({
				layer: true,
				scale: 1.3,
				name: route.gob_addr,
				strokeStyle: "black",
				fillStyle: "black",
				x: route.x + 100,
				y: route.y - 10,
				fromCenter: false,
				text: route.hexpos + "@" + route.gob_addr,
			}).drawRect({
				layer: true,
				strokeStyle: "blue",
				scale: 1.4,
        width: $("#chord").measureText(route.gob_addr).width,
        height: $("#chord").measureText(route.gob_addr).height,
				x: route.x + 100,
				y: route.y - 10,
				fromCenter: false,
				mouseout: function() {
				  console.log("out");
				},
				mouseover: function() {
				  console.log("in");
				},
			});
		});
		if (that.node != null) {
			$("#node_json_addr").text(that.node.json_addr);
			$("#node_gob_addr").text(that.node.gob_addr);
			$("#node_pos").text(that.node.hexpos);
			$("#node_owned_keys").text(that.node.data.OwnedEntries);
			$("#node_held_keys").text(that.node.data.HeldEntries);
		}
	};
	that.routes = [];
	that.node = null;
	that.start = function() {
		that.socket = $.websocket("ws://" + document.location.hostname + ":" + document.location.port + "/ws", {
			open: function() { 
				console.log("socket opened");
			},
			close: function() { 
				console.log("socket closed");
			},
			events: {
				RingChange: function(e) {
					that.routes = [];
					_.each(e.data.routes, function(r) {
						that.routes.push(Node(that, r));
					});
					that.node = Node(that, e.data.description);
					that.drawChord();
				},
				Migration: function(e) {
					console.log(e.data);
				},
				Sync: function(e) {
					console.log(e.data);
				},
				Clean: function(e) {
					console.log(e.data);
				},
			},
		});
	};
	return that;
};

g = new God();

$(function() {
  g.start()
	g.drawChord();
});


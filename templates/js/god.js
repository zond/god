


Route = function(g, data) {
  var that = new Object();
	that.data = data;
	that.addr = data.Addr;
	that.angle = Big(3 * Math.PI / 2).minus($.base64.decode2big(data.Pos).div(g.maxPos).times(Math.PI * 2)).toFixed();
	that.hexpos = "";
	_.each($.base64.decode2b(data.Pos), function(b) {
	  var thishex = b.toString(16);
		while (thishex.length < 2) {
		  thishex = "0" + thishex;
		}
	  that.hexpos += thishex;
	});
	while (that.hexpos.length < 32) {
	  that.hexpos = "0" + that.hexpos;
	}
	return that;
}

God = function() {
  var that = new Object();
	that.maxPos = Big(1).times(Big(256).pow(16));
  that.nodes = [];
	that.cx = 1000;
	that.cy = 1000;
	that.r = 800;
  that.drawChord = function() {
	  $("#chord").clearCanvas().drawArc({
		  strokeStyle: "black",
			x: that.cx,
			y: that.cy,
			radius: that.r,
		}).drawLine({
		  strokeStyle: "black",
		  x1: that.cx, y1: 180,
			x2: that.cx, y2: 220,
		});
		_.each(that.routes, function(route) {
			var x = that.cx + Math.cos(route.angle) * that.r;
			var y = that.cy + Math.sin(route.angle) * that.r;
			$("#chord").drawArc({
				fillStyle: "black",
				x: x,
				y: y,
				radius: 10,
			}).drawText({
				strokeStyle: "black",
				fillStyle: "black",
				scale: 1.3,
				x: x + 100,
				y: y - 10,
				fromCenter: false,
				text: route.hexpos + "@" + route.addr,
			});
		});
	};
	that.routes = [];
	that.node = {};
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
						that.routes.push(Route(that, r));
					});
					that.node = e.data.description;
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


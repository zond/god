
Node = function(g, data) {
  var that = new Object();
	that.data = data;
	that.gob_addr = data.Addr;
	var m = /^(.*):(.*)$/.exec(that.gob_addr);
	that.json_addr = m[1] + ":" + (1 + parseInt(m[2]));
	that.hexpos = "";
	_.each($.base64.decode2b(data.Pos), function(b) {
	  var thishex = b.toString(16);
		while (thishex.length < 2) {
		  thishex = "0" + thishex;
		}
	  that.hexpos += thishex;
	});
	var x_y = g.getPos(data.Pos);
	that.x = x_y[0];
	that.y = x_y[1];
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
	that.last_route_redraw = new Date().getTime();
	that.last_route_update = new Date().getTime();
	that.last_meta_redraw = new Date().getTime();
	that.last_meta_update = new Date().getTime();
	that.getPos = function(b64) {
		var angle = Big(3 * Math.PI / 2).plus($.base64.decode2big(b64).div(that.maxPos).times(Math.PI * 2)).toFixed();
		return [that.cx + Math.cos(angle) * that.r, that.cy + Math.sin(angle) * that.r];
	};
  that.drawChord = function() {
		var stage = new createjs.Stage(document.getElementById("chord"));

		var circle = new createjs.Shape();
		circle.graphics.beginStroke(createjs.Graphics.getRGB(0,0,0)).drawCircle(that.cx, that.cy, that.r);
		stage.addChild(circle);

    var dash = new createjs.Shape();
		dash.graphics.beginStroke(createjs.Graphics.getRGB(0,0,0)).moveTo(that.cx, that.cy - that.r - 30).lineTo(that.cx, that.cy - that.r + 30);
		stage.addChild(dash);

		if (that.last_route_update > that.last_route_redraw) {
			$("#nodes .node").remove();
		}
		for (var addr in that.node_by_addr) {
			var route = that.node_by_addr[addr];    
			var spot = new createjs.Shape();
			spot.graphics.beginStroke(createjs.Graphics.getRGB(0,0,0)).beginFill(createjs.Graphics.getRGB(0,0,0)).drawCircle(route.x, route.y, 20);
			stage.addChild(spot);
			var label = new createjs.Text(route.hexpos + "@" + route.gob_addr, "bold 25px Courier");
			label.x = route.x + 30;
			label.y = route.y - 10;
			stage.addChild(label);
			if (that.last_route_update > that.last_route_redraw) {
				$("#nodes").append($("<tr data-addr=\"" + route.json_addr + "\" class=\"node\"><td>" + route.gob_addr + "</td><td>" + route.hexpos + "</td></tr>"));
			}
		}
    if (that.last_route_update > that.last_route_redraw) {
			$(".node").click(function(e) {
			  that.selectNodeWithAddr($(e.target).parent().attr('data-addr'));
			});
			that.last_route_redraw = new Date().getTime();
		}

    var alphaCut = 300;
    var newAnimations = [];
		var now = new Date().getTime()
		_.each(that.animations, function(animation) {
		  if (animation.ttl > now) {
			  newAnimations.push(animation);
			}
			var left = animation.ttl - now;
			var alpha = 1;
			if (left < alphaCut) {
			  alpha = left / alphaCut;
			}
		  var line = new createjs.Shape()
			var gr = line.graphics.beginStroke(createjs.Graphics.getRGB(animation.color[0], animation.color[1], animation.color[2], alpha)).setStrokeStyle(animation.strokeWidth, animation.caps);
			if (animation.key != null) {
				gr.moveTo(animation.source[0], animation.source[1]).quadraticCurveTo(animation.key[0], animation.key[1], animation.destination[0], animation.destination[1]);
			} else {
				gr.moveTo(animation.source[0], animation.source[1]).lineTo(animation.destination[0], animation.destination[1]);
			}
			stage.addChild(line);
		});
		that.animations = newAnimations;

		stage.update();

    if (that.last_meta_update > that.last_meta_redraw) {
			if (that.node != null) {
				$("#node_json_addr").text(that.node.json_addr);
				$("#node_gob_addr").text(that.node.gob_addr);
				$("#node_pos").text(that.node.hexpos);
				$("#node_owned_keys").text(that.node.data.OwnedEntries);
				$("#node_held_keys").text(that.node.data.HeldEntries);
			}
			that.last_meta_redraw = new Date().getTime();
		}
	};
	that.selectNodeWithAddr = function(addr) {
	  for (var a in that.node_by_addr) {
		  if (a == addr) {
			  that.node = that.node_by_addr[a];
				that.last_meta_update = new Date().getTime();
			}
		}
	};
	that.animations = [];
	that.animate = function(item) {
	  if (that.animations.length < 50) {
		  that.animations.push(item);
		}
	};
	that.opened_sockets = {};
	that.node_by_addr = {};
	that.node = null;
	that.open_socket = function(addr) {
	  addr = addr.replace('localhost', '127.0.0.1');
	  if (that.opened_sockets[addr] == null) {
			that.opened_sockets[addr] = true;
			$.websocket("ws://" + addr + "/ws", {
				open: function() { 
					console.log("socket to " + addr + " opened");
				},
				close: function() { 
				  delete(that.opened_sockets[addr]);
					delete(that.node_by_addr[addr]);
					console.log("socket to " + addr + " closed");
				},
				events: {
					RingChange: function(e) {
						_.each(e.data.routes, function(r) {
							var node = new Node(that, r);
							if (that.node_by_addr[node.json_addr] == null) {
								that.open_socket(node.json_addr);
							}
						});
						var newNode = new Node(that, e.data.description);
						var oldNode = that.node_by_addr[newNode.json_addr];
						if (oldNode == null || JSON.stringify(newNode) != JSON.stringify(oldNode)) {
							that.last_route_update = new Date().getTime();
							that.node_by_addr[newNode.json_addr] = newNode;
						}
						if (that.node == null) {
							that.last_meta_update = new Date().getTime();
						  that.node = newNode;
						}
					},
					Comm: function(e) {
						var item = {
							source: that.getPos(e.data.source.Pos),
							destination: that.getPos(e.data.destination.Pos),
							ttl: new Date().getTime() + 300,
							color: [0,0,200],
							strokeWidth: 3,
							caps: 0,
						};
						if (/HashTree/.exec(e.data.type) != null) {
							item.color = [200,0,0];
						}
						if (e.data.key != null) {
							item.key = that.getPos(e.data.key);
						}
						if (e.data.sub_key != null) {
							item.sub_key = that.getPos(e.data.sub_key);
						}
						that.animate(item);
					},
					Sync: function(e) {
						var item = {
							source: that.getPos(e.data.source.Pos),
							destination: that.getPos(e.data.destination.Pos),
							ttl: new Date().getTime() + 500,
							color: [150,0,150],
							strokeWidth: 5,
							caps: 1,
						};
						that.animate(item);
					},
					Clean: function(e) {
						var item = {
							source: that.getPos(e.data.source.Pos),
							destination: that.getPos(e.data.destination.Pos),
							ttl: new Date().getTime() + 500,
							color: [50,150,0],
							strokeWidth: 4,
							caps: 2,
						};
						that.animate(item);
					},
				},
			});
		}
	};
	that.start = function() {
		window.setInterval(that.drawChord, 100);
		that.open_socket(document.location.hostname + ":" + document.location.port);
	};
	return that;
};

g = new God();

$(function() {
  g.start()
});


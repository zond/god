
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
	that.getPos = function(b64) {
		var angle = Big(3 * Math.PI / 2).plus($.base64.decode2big(b64).div(that.maxPos).times(Math.PI * 2)).toFixed();
		return [that.cx + Math.cos(angle) * that.r, that.cy + Math.sin(angle) * that.r];
	};
  that.drawChord = function() {
		var stage = new createjs.Stage(document.getElementById("chord"));
		stage.enableMouseOver();

		var circle = new createjs.Shape();
		circle.graphics.beginStroke(createjs.Graphics.getRGB(0,0,0)).drawCircle(that.cx, that.cy, that.r);
		stage.addChild(circle);

    var dash = new createjs.Shape();
		dash.graphics.beginStroke(createjs.Graphics.getRGB(0,0,0)).moveTo(that.cx, that.cy - that.r - 30).lineTo(that.cx, that.cy - that.r + 30);
		stage.addChild(dash);

    _.each(that.routes, function(route) {
		  var click = function() {
				window.location = "http://" + route.json_addr;
			};
			var mouseover = function() {
				$("#chord").css({cursor: "pointer"});
			};
			var mouseout = function() {
				$("#chord").css({cursor: "default"}); 
			};
		  var spot = new createjs.Shape();
			spot.graphics.beginStroke(createjs.Graphics.getRGB(0,0,0)).beginFill(createjs.Graphics.getRGB(0,0,0)).drawCircle(route.x, route.y, 20);
			spot.onClick = click;
			spot.onMouseOver = mouseover;
			spot.onMouseOut = mouseout;
			stage.addChild(spot);
			var label = new createjs.Text(route.hexpos + "@" + route.gob_addr, "bold 25px Courier");
			label.onClick = click;
			label.onMouseOver = mouseover;
			label.onMouseOut = mouseout;
			label.x = route.x + 30;
			label.y = route.y - 10;
			stage.addChild(label);
		});

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

		if (that.node != null) {
			$("#node_json_addr").text(that.node.json_addr);
			$("#node_gob_addr").text(that.node.gob_addr);
			$("#node_pos").text(that.node.hexpos);
			$("#node_owned_keys").text(that.node.data.OwnedEntries);
			$("#node_held_keys").text(that.node.data.HeldEntries);
		}
	};
	that.animations = [];
	that.routes = [];
	that.node = null;
	that.start = function() {
		window.setInterval(that.drawChord, 40);
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
				},
				Comm: function(e) {
					var item = {
						source: that.getPos(e.data.source.Pos),
						destination: that.getPos(e.data.destination.Pos),
						ttl: new Date().getTime() + 500,
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
				  that.animations.push(item);
				},
				Sync: function(e) {
					var item = {
						source: that.getPos(e.data.source.Pos),
						destination: that.getPos(e.data.destination.Pos),
						ttl: new Date().getTime() + 1000,
						color: [150,0,150],
						strokeWidth: 5,
						caps: 1,
					};
				  that.animations.push(item);
				},
				Clean: function(e) {
					var item = {
						source: that.getPos(e.data.source.Pos),
						destination: that.getPos(e.data.destination.Pos),
						ttl: new Date().getTime() + 1000,
						color: [50,150,0],
						strokeWidth: 4,
						caps: 2,
					};
				  that.animations.push(item);
				},
			},
		});
	};
	return that;
};

g = new God();

$(function() {
  g.start()
});


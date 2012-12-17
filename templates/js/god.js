
God = function() {
  this.nodes = [],
  this.drawChord = function() {
	  $("#chord").drawArc({
		  strokeStyle: "black",
			x: 500,
			y: 500,
			radius: 400,
		}).drawLine({
		  strokeStyle: "black",
		  x1: 500, y1: 80,
			x2: 500, y2: 120,
		});
	};
	return this;
};

$(function() {
	var g = new God();
	g.drawChord();
});


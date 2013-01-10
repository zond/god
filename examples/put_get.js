var http = require('http');

function rpc(endpoint, params, callback) {
  var data = '';
	var content = JSON.stringify(params);
  var req = http.request({
	  hostname: 'localhost',
		port: 9192,
		headers: {
		  'Content-Length': content.length,
		  'Accept': 'application/json',
		},
		path: '/rpc/DHash.' + endpoint,
		method: 'POST',
	}, function(res) {
	  res.setEncoding('utf8');
		res.on('data', function(d) {
		  data += d;
		});
		res.on('end', function() {
			callback(JSON.parse(data));
		});
		res.on('close', function() {
 		  callback(JSON.parse(data));
		});
	});
	req.on('error', function(e) {
		console.log('problem with request: ' + e.message);
	});
	req.write(content);
	req.end();
};

var user = {
  email: 'mail@domain.tld',
	password: 'so secret',
	name: 'john doe',
};

rpc('Put', { 
	Key: new Buffer(user.email).toString('base64'),
	Value: new Buffer(JSON.stringify(user)).toString('base64'),
}, function() {
  rpc('Get', { Key: new Buffer(user.email).toString('base64') }, function(data) {
	  console.log('stored and found', JSON.parse(new Buffer(data.Value, 'base64').toString('utf-8')));
	});
});


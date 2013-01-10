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

function after(n, callback) {
  var count = n;
	return function() {
	  count--;
		if (count == 0) {
		  callback();
		}
	}
}

var followers_key = new Buffer("mail@domain.tld/followers").toString('base64')
var followees_key = new Buffer("mail@domain.tld/followees").toString('base64')
var followers = ["user1@domain.tld", "user2@domain.tld", "user3@domain.tld"];
var followees = ["user3@domain.tld", "user4@domain.tld"];
var cb = after(followers.length + followees.length, function() {
  rpc('SetExpression', {
		Code: '(I ' + new Buffer(followers_key, 'base64').toString('utf-8') + ' ' + new Buffer(followees_key, 'base64').toString('utf-8') + ')',
	}, function(data) {
	  data.map(function(friend) {
			console.log(new Buffer(friend.Key, 'base64').toString('utf-8'));
		});
	});
});
followers.map(function(follower) {
  rpc('SubPut', {
	  Key: followers_key,
		SubKey: new Buffer(follower).toString('base64'),
	}, cb);
});
followees.map(function(followee) {
  rpc('SubPut', {
	  Key: followees_key,
		SubKey: new Buffer(followee).toString('base64'),
	}, cb);
});
// output: user3@domain.tld

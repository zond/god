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

var key = new Buffer("mail@domain.tld/followers").toString('base64')
var followers = ["follower1@domain.tld", "follower2@domain.tld", "follower3@domain.tld"];
var cb = after(followers.length, function() {
  rpc('SliceLen', {
    Key: key,
    Len: 1,
  }, function(data) {
    console.log('my first follower is', new Buffer(data[0].Key, 'base64').toString('utf-8'));
  });
  rpc('ReverseSliceLen', {
    Key: key,
    Len: 2,
  }, function(data) {
    console.log('my last two followers are', new Buffer(data[1].Key, 'base64').toString('utf-8'), 'and', new Buffer(data[0].Key, 'base64').toString('utf-8'));
  });
});
followers.map(function(follower) {
  rpc('SubPut', {
    Key: key,
    SubKey: new Buffer(follower).toString('base64'),
  }, cb);
});
// output: my first follower is follower1@domain.tld
// output: my last two followers are user2@domain.tld and user3@domain.tld

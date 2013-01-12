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
};

// convert integer to base64 encoded bytes
function i2b(i) {
  var b = new Buffer(4);
  b.writeInt32BE(i, 0);
  return b.toString('base64');
};

// convert base64 encoded bytes to an integer
function b2i(b) {
  var b = new Buffer(b, 'base64');
  return b.readInt32BE(0);
};

// dump a bunch of scores
var key = new Buffer("score_by_email").toString('base64')
var scores = {
  "mail1@domain.tld": i2b(1234),
  "mail2@domain.tld": i2b(3),
  "mail3@domain.tld": i2b(61),
  "mail4@domain.tld": i2b(1121),
  "mail5@domain.tld": i2b(9192),
  "mail6@domain.tld": i2b(5123),
  "mail7@domain.tld": i2b(44),
  "mail8@domain.tld": i2b(6),
};
// define a callback that
var cb = after(9, function() {
  // fetches a slice of len 3 in reverse from the mirror tree
  rpc('MirrorReverseSliceLen', {
    Key: key,
    Len: 3,
  }, function(data) {
    // and prints it
    console.log("top three scores");
    data.map(function(score) {
      console.log(new Buffer(score.Value, 'base64').toString('utf-8'), b2i(score.Key));
    });
  });
});
// make the score tree mirrored
rpc('SubAddConfiguration', {
  Key: 'mirrored',
  Value: 'yes',
}, cb);
// and insert the scores
for (var email in scores) {
  rpc('SubPut', {
    Key: key,
    SubKey: new Buffer(email).toString('base64'),
    Value: scores[email],
  }, cb);
}
// output: top three scores
// output: mail5@domain.tld 9192
// output: mail6@domain.tld 5123
// output: mail1@domain.tld 1234

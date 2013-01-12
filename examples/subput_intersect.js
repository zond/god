var http = require("http");

function rpc(endpoint, params, callback) {
  var data = "";
  var content = JSON.stringify(params);
  var req = http.request({
    hostname: "localhost",
    port: 9192,
    headers: {
      "Content-Length": content.length,
      "Accept": "application/json",
    },
    path: "/rpc/DHash." + endpoint,
    method: "POST",
  }, function(res) {
    res.setEncoding("utf8");
    res.on("data", function(d) {
      data += d;
    });
    res.on("end", function() {
      callback(JSON.parse(data));
    });
    res.on("close", function() {
      callback(JSON.parse(data));
    });
  });
  req.on("error", function(e) {
    console.log("problem with request: " + e.message);
  });
  req.write(content);
  req.end();
};

// run the callback the n'th time the return value is executed
function after(n, callback) {
  var count = n;
  return function() {
    count--;
    if (count == 0) {
      callback();
    }
  }
}

// lets store followers here
var followers_key = new Buffer("mail@domain.tld/followers").toString("base64")
// and followees here
var followees_key = new Buffer("mail@domain.tld/followees").toString("base64")
// define a few of each
var followers = ["user1@domain.tld", "user2@domain.tld", "user3@domain.tld"];
var followees = ["user3@domain.tld", "user4@domain.tld"];
// define a callback that...
var cb = after(followers.length + followees.length, function() {
  // runs an intersection of the followers and followees
  rpc("SetExpression", {
    Code: "(I " + new Buffer(followers_key, "base64").toString("utf-8") + " " + new Buffer(followees_key, "base64").toString("utf-8") + ")",
  }, function(data) {
    // and just prints them
    data.map(function(friend) {
      console.log(new Buffer(friend.Key, "base64").toString("utf-8"));
    });
  });
});
// insert the followers
followers.map(function(follower) {
  rpc("SubPut", {
    Key: followers_key,
    SubKey: new Buffer(follower).toString("base64"),
  }, cb);
});
// insert the followees
followees.map(function(followee) {
  rpc("SubPut", {
    Key: followees_key,
    SubKey: new Buffer(followee).toString("base64"),
  }, cb);
});
// output: user3@domain.tld

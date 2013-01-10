$.ajax('/rpc/DHash.Put', {
 type: 'POST',
 contentType: 'application/json; charset=UTF-8',
 data: JSON.stringify({
  Key: $.base64.encode(email),
  Value: $.base64.encode(JSON.stringify({Email: email, Password: crypt(password), Name: name})),
 }),
 success: callback,
 dataType: 'json',
});


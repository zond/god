conn := client.MustConn("localhost:9191")
user := User{
	Email: email,
	Password: crypt(password),
	Name: name,
}
if bytes, err := json.Marshal(user); err != nil {
	panic(err)
} else {
	conn.Put(murmur.HashString(user.Email), bytes)
}

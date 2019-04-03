# broker
A message broker in go. A toy project. Most fun part is using closures instead of mutexes for a shared data src without any race conditions.

# usage
The message protocol is `<op>;[<queue>];[<text>]`
```bash
# run server
$ go run main.go
# run client
$ nc localhost 12001
pub;cats;bixa is a kitty
ok
sub;cats;
ok
bixa is a kitty
```

# tests
```bash
$ go test ./pkg/broker/...
```

# todos
- [x] pub
- [x] sub
- [x] create queues on the fly
- [ ] persistance and ack
- [ ] msg max len
- [ ] less messy msg protocol
- [ ] handle empty msg text
- [ ] concurrent dispatch
- [ ] avoid sub dupes
- [x] add server component
- [x] add brokerConf
- [ ] server conf
- [x] replace net.Conn with io.Writer

# license
MIT

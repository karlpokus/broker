# broker
A message broker in go. A toy project.

# fun parts
- using closures instead of mutexes
- trying to make the broker interface as simple as a client just providing an io.Reader and an io.Writer (not there just yet) which means you can add the broker atop any valid implementation - not just a tcp server like in this repo.
- figuring out if the store can be a interface

# usage
The message protocol is `ping|<op>;<queue>;[<text>]`
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
ping # extends the timeout
ok
```

# tests
```bash
$ go test ./pkg/broker/... [-race -bench=. -cover]
```

# todos
- [x] pub
- [x] sub
- [x] create queues on the fly
- [ ] persistance and ack
- [ ] msg max len
- [ ] less messy msg protocol
- [x] handle empty msg text
- [x] concurrent delivery to subscriber
- [x] avoid sub dupes
- [x] add server component
- [x] add brokerConf
- [x] replace net.Conn with io.Writer
- [x] add benchmark for `NewId`
- [ ] try reading [1]byte to check connection before write - requires a io.ReadWriter
- [x] store refs to subscribed queues to make the blocking op to remove subscriber faster
- [x] add ping
- [x] detach server component from broker
- [x] change broker interface to a client with an io.Reader and an io.Writer

# license
MIT

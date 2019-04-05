package bufw

import (
	"bytes"
	"sync"
)

// bufw implements io.Writer
// safe for concurrent use
type bufw struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	written chan bool
}

// Write writes to an internal buffer for later inspection
// also writes to the written chan if it has been instantiated
func (bw *bufw) Write(b []byte) (n int, err error) {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	bw.buf.Write(b)
	if bw.written != nil {
		bw.written <- true
	}
	return len(b), nil
}

// Drain resets-, and returns the buffer contents
func (bw *bufw) Drain() string {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	s := bw.buf.String()
	bw.buf.Reset()
	return s
}

// Wait blocks on the written chan, used for synchronization
// must use if written chan has been instantiated or the Write call will block
func (bw *bufw) Wait() bool {
	return <-bw.written
}

// New returns a bufw type and instantiates the written chan if b is true
func New(b bool) *bufw {
	w := &bufw{}
	if b {
		w.written = make(chan bool)
	}
	return w
}

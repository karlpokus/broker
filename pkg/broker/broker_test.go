package broker

import (
	"bytes"
	"sync"
	"testing"
)

type mockWriter struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	written chan bool
}

// Write writes to an internal buffer for later inspection
// also writes to the written chan if it has been instantiated
func (mw *mockWriter) Write(b []byte) (n int, err error) {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	mw.buf.Write(b)
	if mw.written != nil {
		mw.written <- true
	}
	return len(b), nil
}

// Drain resets-, and returns the buffer contents
func (mw *mockWriter) Drain() string {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	s := mw.buf.String()
	mw.buf.Reset()
	return s
}

// Wait blocks on the written chan, used for synchronization
// must use if written chan has been instantiated or the Write call will block
func (mw *mockWriter) Wait() bool {
	return <-mw.written
}

func TestPubSub(t *testing.T) {
	bkr := NewBroker(&brokerConf{})
	w := &mockWriter{
		written: make(chan bool),
	}
	id, err := NewId()
	if err != nil {
		t.Errorf("%s", err)
	}
	// sub
	m := []byte("sub;cats;")
	err = bkr.Parse(m, w, id)
	if err != nil {
		t.Errorf("%s", err)
	}
	bkr.ops <- func(s storage) {
		_, ok := s["cats"].subs[id]
		if !ok {
			t.Errorf("%s should be present in queue subs", id)
		}
	}
	// subDupe
	err = bkr.Parse(m, w, id)
	if err != subDupe {
		t.Errorf("got %s, want %s", err, subDupe)
	}
	// pub
	m = []byte("pub;cats;bixa")
	err = bkr.Parse(m, w, id)
	if err != nil {
		t.Errorf("%s", err)
	}
	bkr.ops <- func(s storage) {
		msgs := len(s["cats"].msgs)
		if msgs != 0 {
			t.Errorf("got %d, want %d", msgs, 0)
		}
	}
	w.Wait()
	want := "bixa"
	got := w.Drain()
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

// avoid compiler optimisations
var gid uuid

func BenchmarkNewId(b *testing.B) {
	var id uuid
	for n := 0; n < b.N; n++ {
		id, _ = NewId()
	}
	gid = id
}

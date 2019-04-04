package broker

import (
	"bytes"
	"sync"
	"testing"
)

// mockWriter writes to internal buffer for later inspection
// internal buffer is protected by mutex
type mockWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (mw *mockWriter) Write(b []byte) (n int, err error) {
	mw.mu.Lock()
	defer mw.mu.Unlock()
	mw.buf.Write(b)
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

func TestPubSub(t *testing.T) {
	bkr := NewBroker(&brokerConf{})
	w := &mockWriter{}
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
	want := "bixa"
	bkr.ops <- func(s storage) {
		got := s["cats"].msgs[0]
		if got != want {
			t.Errorf("got %s, want %s", got, want)
		}
	}
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

package broker

import (
	"testing"

	"github.com/karlpokus/broker/pkg/bufw"
)

func TestPubSub(t *testing.T) {
	bkr := NewBroker(&brokerConf{})
	w := bufw.New(true)
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
	if err != subDupeErr {
		t.Errorf("got %s, want %s", err, subDupeErr)
	}

	// pub 1
	m = []byte("pub;cats;bixa")
	err = bkr.Parse(m, w, id)
	if err != nil {
		t.Errorf("%s", err)
	}
	bkr.ops <- msgsN(t)
	w.Wait()
	want := "bixa"
	got := w.Drain()
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}

	// pub 2
	m = []byte("pub;cats;rex")
	err = bkr.Parse(m, w, id)
	if err != nil {
		t.Errorf("%s", err)
	}
	bkr.ops <- msgsN(t)
	w.Wait()
	want = "rex"
	got = w.Drain()
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func msgsN(t *testing.T) opFunc {
	return func(s storage) {
		msgs := len(s["cats"].msgs)
		if msgs != 0 {
			t.Errorf("got %d, want %d", msgs, 0)
		}
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

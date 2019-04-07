package broker

import (
	"testing"

	"github.com/karlpokus/bufw"
)

// What we are testing in TestPubSub:
// 1. client attempt to subscribe to a queue
//  expect
//  a. client id to be present in queue subs
//  b. client subs to contain queue name
// 2. do 1 again
//  expect subDupeErr
// 3. same client attempt to publish a msg to a queue
//  expect
//  a. queue msgs to be empty (should be delivered)
//  b. msg text written to the client writer
// 4. do 3 again
//  expect same result
// 5. remove subscription
//  expect client id to be missing from queue subs
func TestPubSub(t *testing.T) {
	bkr := NewBroker(&brokerConf{})
	w := bufw.New(true)
	cnt, err := newClient(w)
	if err != nil {
		t.Errorf("%s", err)
	}

	// sub
	buf := []byte("sub;cats;")
	err = bkr.Parse(buf, cnt)
	if err != nil {
		t.Errorf("%s", err)
	}
	bkr.ops <- func(s storage) {
		_, ok := s["cats"].subs[cnt.id]
		if !ok {
			t.Errorf("%s should be present in queue subs", cnt.id)
		}
	}
	if !sliceContains(cnt.subs, "cats") {
		t.Errorf("%v should contain queue name", cnt.subs)
	}

	// subDupe
	err = bkr.Parse(buf, cnt)
	if err != subDupeErr {
		t.Errorf("got %s, want %s", err, subDupeErr)
	}

	// pub 1
	buf = []byte("pub;cats;bixa")
	err = bkr.Parse(buf, cnt)
	if err != nil {
		t.Errorf("%s", err)
	}
	bkr.ops <- msgsN(t)
	w.Wait()
	want := "bixa"
	got := w.String()
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}

	// pub 2
	buf = []byte("pub;cats;rex")
	err = bkr.Parse(buf, cnt)
	if err != nil {
		t.Errorf("%s", err)
	}
	bkr.ops <- msgsN(t)
	w.Wait()
	want = "rex"
	got = w.String()
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}

	// remove subscription
	bkr.removeSubs(cnt)
	bkr.ops <- func(s storage) {
		_, ok := s["cats"].subs[cnt.id]
		if ok {
			t.Errorf("%s should not be present in queue subs", cnt.id)
		}
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

func sliceContains(sl []string, q string) bool {
	for _, s := range sl {
		if s == q {
			return true
		}
	}
	return false
}

// avoid compiler optimisations
var gid string

func BenchmarkNewId(b *testing.B) {
	var id string
	for n := 0; n < b.N; n++ {
		id, _ = newId()
	}
	gid = id
}

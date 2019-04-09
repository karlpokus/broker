package broker

import (
	"strings"
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
	bkr := NewBroker(&Conf{})
	w := bufw.New(true)
	cnt, err := NewClient(nil, w) // create reader later
	if err != nil {
		t.Errorf("%s", err)
	}

	// sub
	cnt.r = strings.NewReader("sub;cats;")
	err, fatal := bkr.Handle(cnt)
	if err != nil || fatal {
		t.Errorf("%s", err)
	}
	bkr.ops <- subscribed(true, "cats", cnt, t)
	if !sliceContains(cnt.subs, "cats") {
		t.Errorf("%v should contain queue name", cnt.subs)
	}

	// subDupe
	cnt.r = strings.NewReader("sub;cats;")
	err, fatal = bkr.Handle(cnt)
	if fatal {
		t.Errorf("subDupe should not be fatal")
	}
	if err != subDupeErr {
		t.Errorf("got %s, want %s", err, subDupeErr)
	}

	// pub 1
	cnt.r = strings.NewReader("pub;cats;bixa")
	err, fatal = bkr.Handle(cnt)
	if err != nil || fatal {
		t.Errorf("%s", err)
	}
	bkr.ops <- msgsN(0, t)
	w.Wait()
	want := "bixa"
	got := w.String()
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}

	// pub 2
	cnt.r = strings.NewReader("pub;cats;rex")
	err, fatal = bkr.Handle(cnt)
	if err != nil || fatal {
		t.Errorf("%s", err)
	}
	bkr.ops <- msgsN(0, t)
	w.Wait()
	want = "rex"
	got = w.String()
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}

	// remove subscription
	cnt.r = strings.NewReader("")
	err, fatal = bkr.Handle(cnt)
	if !fatal {
		t.Errorf("%s should be fatal", err)
	}
	bkr.ops <- subscribed(false, "cats", cnt, t)
}

func subscribed(b bool, q string, cnt *client, t *testing.T) opFunc {
	return func(s storage) {
		_, ok := s[q].subs[cnt.id]
		if ok != b {
			t.Errorf("%s in %s should be %t", cnt.id, q, b)
		}
	}
}

func msgsN(n int, t *testing.T) opFunc {
	return func(s storage) {
		msgs := len(s["cats"].msgs)
		if msgs != n {
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

package broker

import "testing"

var testTable = []struct {
	b   []byte
	m   msg
	err error
}{
	{[]byte("no-semi-colons"), msg{}, InvalidMsg},
	{[]byte("foo;doo;"), msg{}, InvalidOp},
	{[]byte("pub;;"), msg{}, InvalidQueue},
	{[]byte("ack;;"), msg{"ack", "", ""}, nil},
	{[]byte("pub;cats;bixa the kitty"), msg{"pub", "cats", "bixa the kitty"}, nil},
	{[]byte("pub;cats;"), msg{"pub", "cats", ""}, nil},
	{[]byte("sub;cats;"), msg{"sub", "cats", ""}, nil},
}

func TestParser(t *testing.T) {
	for _, tt := range testTable {
		res, err := parse(tt.b)
		if *res != tt.m {
			t.Errorf("want %v, got %v", tt.m, res)
		}
		if err != tt.err {
			t.Errorf("want %v, got %v", tt.err, err)
		}
	}
}

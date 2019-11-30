package flatfile

import (
	"os"
	"testing"
)

func TestHeader(t *testing.T) {

	// TODO TestHeader
	const (
		headertest = "test/headertest"
	)
	defer os.RemoveAll(headertest)

	hdr := newHeader(headertest)
	if _, err := hdr.Open(true, false); err != nil {
		t.Fatal(err)
	}

	c := hdr.Select(true, 16)
	hdr.Cache(c, []byte{}, 1024)

	if err := hdr.Close(); err != nil {
		t.Fatal(err)
	}
}

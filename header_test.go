package flatfile

import "testing"

func TestHeader(t *testing.T) {

	const (
		HeaderFilename = "test/headertest"
	)

	hdr := newHeader(HeaderFilename)
	if _, err := hdr.Open(true, false); err != nil {
		t.Fatal(err)
	}

	c := hdr.Select(true, 16)
	hdr.Cache(c, []byte{}, 1024)

	if err := hdr.Close(); err != nil {
		t.Fatal(err)
	}
}

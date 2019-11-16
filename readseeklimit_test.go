package flatfile

import (
	"bytes"
	"io"
	"os"
	"testing"
)

func TestReadSeekLimiter(t *testing.T) {
	file, err := os.OpenFile("testrsl.dat", os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	var testdata = []byte("abcdefghijklmno")
	if _, err := file.Write(testdata); err != nil {
		t.Fatal(err)
	}

	rsl, err := NewReadSeekLimiter(file, 5, 5)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := rsl.Seek(-1, os.SEEK_SET); err == nil {
		t.Fatal("broken SET seek didn't error")
	}

	if _, err := rsl.Seek(10, os.SEEK_CUR); err == nil {
		t.Fatal("broken CUR seek didn't error")
	}

	if _, err := rsl.Seek(1, os.SEEK_END); err == nil {
		t.Fatal("broken END seek didn't error")
	}

	buf := make([]byte, 5)
	if _, err := rsl.Read(buf); err != nil {
		t.Fatal(err)
	}
	if bytes.Compare(testdata[5:10], buf) != 0 {
		t.Fatal("missmatch")
	}

	buf = make([]byte, 10)

	if _, err := rsl.Read(buf); err != io.EOF {
		t.Logf("limit exceeded: %w", err)
	}

}

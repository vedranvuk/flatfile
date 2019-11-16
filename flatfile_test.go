// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package flatfile

import (
	"os"
	"testing"

	"github.com/vedranvuk/strings"
)

// TestFlatFileBasicRW writes predefined data, gets and checks it
// then reopens the file, checking data again.
func TestFlatFileBasicRW(t *testing.T) {

	if err := os.RemoveAll("test"); err != nil {
		t.Fatal(err)
	}

	data := make(map[string]string)
	for i := 0; i < 1000; i++ {
		key := strings.RandomString(true, true, true, 8)
		val := strings.RandomString(true, true, true, 8)
		data[key] = val
	}

	options := NewOptions()
	options.PreallocatePages = false
	ff, err := Open("test", options)
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range data {
		if err := ff.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}
	for k, v := range data {
		buf, err := ff.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if string(buf) != v {
			t.Fatalf("missmatch: want '%s', got '%s'\n", k, string(buf))
		}
	}

	if err := ff.Close(); err != nil {
		t.Fatal(err)
	}

	ff, err = Open("test", NewOptions())
	if err != nil {
		t.Fatal(err)
	}

	for k, v := range data {
		buf, err := ff.Get([]byte(k))
		if err != nil {
			t.Fatal(err)
		}
		if string(buf) != v {
			t.Fatalf("missmatch: want '%s', got '%s'\n", k, string(buf))
		}
	}

	if err := ff.Close(); err != nil {
		t.Fatal(err)
	}
}

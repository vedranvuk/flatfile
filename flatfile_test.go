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

	testdir := "test/basicrw"
	if err := os.RemoveAll(testdir); err != nil {
		t.Fatal(err)
	}

	data := make(map[string]string)
	for i := 0; i < 1024; i++ {
		key := strings.RandomString(true, true, true, 8)
		val := strings.RandomString(true, true, true, 8)
		data[key] = val
	}

	options := NewOptions()
	options.PreallocatePages = true
	options.MaxPageSize = 1024
	ff, err := Open(testdir, options)
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

	ff, err = Open(testdir, NewOptions())
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

func TestCRUD(t *testing.T) {

	// remove temp files
	testdir := "test/crud"
	if err := os.RemoveAll(testdir); err != nil {
		t.Fatal(err)
	}
	testmirrordir := "test/crudmirror"
	if err := os.RemoveAll(testmirrordir); err != nil {
		t.Fatal(err)
	}
	// init test data
	data := make(map[string]string)
	data["key1"] = "dataK"
	data["key2"] = "dataF"
	data["key3"] = "dataJ"
	// create flatfile
	options := NewOptions()
	options.MaxPageSize = -1
	options.MirrorDir = testmirrordir
	ff, err := Open(testdir, options)
	if err != nil {
		t.Fatal(err)
	}
	// Put 3 pairs
	for key, val := range data {
		if err := ff.Put([]byte(key), []byte(val)); err != nil {
			t.Fatal(err)
		}
	}
	for loop := 0; loop < 10; loop++ {
		// save, close, open
		if err := ff.Reopen(); err != nil {
			t.Fatal(err)
		}
		// check all keys
		for dkey, dval := range data {
			blob, err := ff.Get([]byte(dkey))
			if err != nil {
				t.Fatal(err)
			}
			if string(blob) != dval {
				t.Fatal("missmatch")
			}
		}
		// modify 2nd val
		data["key2"] = "dataH"
		if err := ff.Modify([]byte("key2"), []byte("dataH")); err != nil {
			t.Fatal(err)
		}
		// check 2nd val
		blob, err := ff.Get([]byte("key2"))
		if err != nil {
			t.Fatal(err)
		}
		if string(blob) != data["key2"] {
			t.Fatal("missmatch")
		}
		// save, close, open
		if err := ff.Reopen(); err != nil {
			t.Fatal(err)
		}
		// check 2nd val
		blob, err = ff.Get([]byte("key2"))
		if err != nil {
			t.Fatal(err)
		}
		if string(blob) != data["key2"] {
			t.Fatal("missmatch")
		}
		// del 1st val
		delete(data, "key1")
		if err := ff.Delete([]byte("key1")); err != nil {
			t.Fatal(err)
		}
		// save, close, open
		if err := ff.Reopen(); err != nil {
			t.Fatal(err)
		}
		// check all keys
		for dkey, dval := range data {
			blob, err := ff.Get([]byte(dkey))
			if err != nil {
				t.Fatal(err)
			}
			if string(blob) != dval {
				t.Fatal("missmatch")
			}
		}
		// add used key
		data["key1"] = "dataA"
		if err := ff.Put([]byte("key1"), []byte("dataA")); err != nil {
			t.Fatal(err)
		}
		// check all keys
		for dkey, dval := range data {
			blob, err := ff.Get([]byte(dkey))
			if err != nil {
				t.Fatal(err)
			}
			if string(blob) != dval {
				t.Fatal("missmatch")
			}
		}
		// save, close, open
		if err := ff.Reopen(); err != nil {
			t.Fatal(err)
		}
		// check all keys
		for dkey, dval := range data {
			blob, err := ff.Get([]byte(dkey))
			if err != nil {
				t.Fatal(err)
			}
			if string(blob) != dval {
				t.Fatal("missmatch")
			}
		}
	}
	// close
	if err := ff.Close(); err != nil {
		t.Fatal(err)
	}
}

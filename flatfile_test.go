// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package flatfile

import (
	"fmt"
	"os"
	"testing"

	"github.com/vedranvuk/strings"
)

// TestFlatFileBasicRW writes predefined data, gets and checks it
// then reopens the file, checking data again.
func TestFlatFileBasicRW(t *testing.T) {

	testdir := "test/basicrw"
	defer os.RemoveAll(testdir)

	data := make(map[string]string)
	for i := 0; i < 10; i++ {
		key := strings.RandomString(true, true, true, 8)
		val := strings.RandomString(true, true, true, 8)
		data[key] = val
	}

	options := NewOptions()
	options.PreallocatePages = true
	options.MaxPageSize = 1024
	options.UseIntents = true
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
	defer os.RemoveAll(testdir)

	testmirrordir := "test/crudmirror"
	defer os.RemoveAll(testmirrordir)

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

func TestExtensive(t *testing.T) {

	const (
		TestDataSize = 256
		CellDataSize = 8
	)

	testdata := make(map[string]string)
	for i := 0; i < TestDataSize; i++ {
		key := fmt.Sprintf("data%.4d", i)
		dat := make([]byte, CellDataSize)
		testdata[key] = string(dat)
	}

	const (
		testdir   = "test/extensive"
		mirrordir = "test/extensive/mirror"
	)
	defer os.RemoveAll(testdir)
	defer os.RemoveAll(mirrordir)

	options := NewOptions()
	options.MirrorDir = mirrordir
	options.CRC = true
	options.MaxCacheMemory = 1024
	options.CachedWrites = true
	options.MaxPageSize = 4096
	options.PreallocatePages = true
	options.PersistentHeader = true
	options.Immutable = false
	options.SyncWrites = false
	options.ZeroPadDeleted = true
	options.CompactHeader = true
	options.UseIntents = true

	ff, err := Open(testdir, options)
	if err != nil {
		t.Fatal(err)
	}

	ff.Close()

}

func TestWalk(t *testing.T) {

	testdir := "test/walk"
	defer os.RemoveAll(testdir)

	data := make(map[string]string)
	for i := 0; i < 10; i++ {
		key := strings.RandomString(true, true, true, 8)
		val := strings.RandomString(true, true, true, 8)
		data[key] = val
	}

	ff, err := Open(testdir, NewOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer ff.Close()

	for k, v := range data {
		if err := ff.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	if err := ff.Walk(func(key, val []byte) bool {
		if string(val) != string(data[string(key)]) {
			t.Fatalf("walk failed, want: '%s', got '%s'\n", string(val), string(data[string(key)]))
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
}

func TestKeys(t *testing.T) {

	testdir := "test/keys"
	defer os.RemoveAll(testdir)

	data := make(map[string]string)
	for i := 0; i < 1024; i++ {
		key := strings.RandomString(true, true, true, 8)
		val := strings.RandomString(true, true, true, 8)
		data[key] = val
	}

	ff, err := Open(testdir, NewOptions())
	if err != nil {
		t.Fatal(err)
	}
	defer ff.Close()

	for k, v := range data {
		if err := ff.Put([]byte(k), []byte(v)); err != nil {
			t.Fatal(err)
		}
	}

	keys := ff.Keys()
	if len(keys) != len(data) {
		t.Fatalf("keys failed, want %d keys, got %d\n", len(data), len(keys))
	}

	for datak, datav := range data {
		blob, err := ff.Get([]byte(datak))
		if err != nil {
			t.Fatal(err)
		}
		if string(blob) != datav {
			t.Fatalf("keys failed, want '%s', got '%s'", string(datav), string(blob))
		}
	}
}

func benchmarkGet(b *testing.B, options *Options) {

	b.StopTimer()

	const (
		testdir = "test/benchmark/reads"
	)
	defer os.RemoveAll(testdir)

	ff, err := Open(testdir, options)
	if err != nil {
		b.Fatal(err)
	}

	datai := []string{}
	datam := make(map[string]string)
	for i := 0; i < b.N; i++ {
		key := ""
		for {
			key = strings.RandomString(true, true, true, 8)
			if _, ok := datam[key]; !ok {
				break
			}
		}
		val := strings.RandomString(true, true, true, 16)
		datam[key] = val
		datai = append(datai, key)
		if err = ff.Put([]byte(key), []byte(val)); err != nil {
			b.Fatal(err)
		}
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		key := datai[i]
		if _, err := ff.Get([]byte(key)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	if err = ff.Close(); err != nil {
		b.Fatal(err)
	}
}

func benchmarkPut(b *testing.B, options *Options) {

	b.StopTimer()

	const (
		testdir = "test/benchmark/writes"
	)
	defer os.RemoveAll(testdir)

	ff, err := Open(testdir, options)
	if err != nil {
		b.Fatal(err)
	}
	datai := []string{}
	datam := make(map[string]string)
	for i := 0; i < b.N; i++ {
		key := ""
		for {
			key = strings.RandomString(true, true, true, 8)
			if _, ok := datam[key]; !ok {
				break
			}
		}
		val := strings.RandomString(true, true, true, 24)
		datai = append(datai, key)
		datam[key] = val
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		key := datai[i]
		val := datam[key]
		if err := ff.Put([]byte(key), []byte(val)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	if err = ff.Close(); err != nil {
		b.Fatal(err)
	}
}

func benchmarkDelete(b *testing.B, options *Options) {

	b.StopTimer()

	const (
		testdir = "test/benchmark/reads"
	)
	defer os.RemoveAll(testdir)

	ff, err := Open(testdir, options)
	if err != nil {
		b.Fatal(err)
	}

	datai := []string{}
	datam := make(map[string]string)
	for i := 0; i < b.N; i++ {
		key := ""
		for {
			key = strings.RandomString(true, true, true, 8)
			if _, ok := datam[key]; !ok {
				break
			}
		}
		val := strings.RandomString(true, true, true, 16)
		datam[key] = val
		datai = append(datai, key)
		if err = ff.Put([]byte(key), []byte(val)); err != nil {
			b.Fatal(err)
		}
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		key := datai[i]
		if err := ff.Delete([]byte(key)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	if err = ff.Close(); err != nil {
		b.Fatal(err)
	}
}

func benchmarkModify(b *testing.B, options *Options) {

	b.StopTimer()

	const (
		testdir = "test/benchmark/reads"
	)
	defer os.RemoveAll(testdir)

	ff, err := Open(testdir, options)
	if err != nil {
		b.Fatal(err)
	}

	datai := []string{}
	datam := make(map[string]string)
	mdatam := make(map[string]string)
	for i := 0; i < b.N; i++ {
		key := ""
		for {
			key = strings.RandomString(true, true, true, 8)
			if _, ok := datam[key]; !ok {
				break
			}
		}
		val := strings.RandomString(true, true, true, 16)
		mval := ""
		for {
			if mval != val {
				break
			}
		}
		datam[key] = val
		mdatam[key] = mval
		datai = append(datai, key)
		if err = ff.Put([]byte(key), []byte(val)); err != nil {
			b.Fatal(err)
		}
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		key := datai[i]
		mval := mdatam[key]
		if err := ff.Modify([]byte(key), []byte(mval)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	if err = ff.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkPut(b *testing.B) {
	options := NewOptions()
	benchmarkPut(b, options)
}

func BenchmarkGet(b *testing.B) {
	options := NewOptions()
	benchmarkGet(b, options)
}

func BenchmarkDelete(b *testing.B) {
	options := NewOptions()
	benchmarkDelete(b, options)
}

func BenchmarkModify(b *testing.B) {
	options := NewOptions()
	benchmarkModify(b, options)
}

// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"log"
	"os"
	"runtime"
	"time"
	"unsafe"

	"github.com/vedranvuk/flatfile"
)

func run(ff FlatFileInterface) time.Duration {
	locktestoptions := NewLockTestOptions()
	locktestoptions.Verbose = false
	locktestoptions.TestDuration = 10 * time.Second
	locktestoptions.MinGetDelay = 0
	locktestoptions.MaxGetDelay = 0
	locktestoptions.MinPutDelay = 0
	locktestoptions.MaxPutDelay = 0
	locktestoptions.MinDelDelay = 1000
	locktestoptions.MaxDelDelay = 1000
	locktestoptions.MinModDelay = 1000
	locktestoptions.MaxModDelay = 1000
	locktestoptions.MaxActiveR = 10
	locktestoptions.MaxActiveW = 1
	locktestoptions.MaxActiveD = 10
	locktestoptions.MaxActiveM = 10
	locktestoptions.MaxR = 1000
	locktestoptions.MaxW = 1000
	locktestoptions.MaxD = 0
	locktestoptions.MaxM = 0
	locktest := NewLockTest(locktestoptions)
	return locktest.Run(ff)
}

func RunEmu() (dur time.Duration) {
	log.Println("Running emu...")
	runtime.GC()
	PrintMemUsage()
	ffemu := NewFlatFileEmulator()
	dur = run(ffemu)
	PrintMemUsage()
	return
}

func RunForReal() (dur time.Duration) {
	log.Println("Running for real...")
	runtime.GC()
	PrintMemUsage()

	if err := os.RemoveAll("testfile"); err != nil {
		log.Fatalf("Can't clear test data: %v", err)
	}

	options := flatfile.NewOptions()
	options.Immutable = false
	options.SyncWrites = false
	options.PersistentHeader = true
	options.MaxPageSize = 1048576 // 1MB
	// options.MirrorDir = "/home/vedran/backup"

	ff, err := flatfile.Open("testfile", options)
	if err != nil {
		log.Fatal("Open error:", err)
	}

	log.Printf("FlatFile empty size: %d\n", unsafe.Sizeof(ff))
	dur = run(ff)
	log.Printf("FlatFile post-test size: %d\n", unsafe.Sizeof(*ff))

	if ff.Close(); err != nil {
		log.Fatalf("FATAL: Close: %v\n", err)
	}

	PrintMemUsage()
	return
}

func main() {
	emu := RunEmu()
	rly := time.Duration(0)
	rly = RunForReal()
	for i := 0; i < 5; i++ {
	}

	log.Println("---------------------------------------------")
	log.Printf("Emu took %s, Rly took %s\n", emu, rly)
}

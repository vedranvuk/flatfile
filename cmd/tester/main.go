// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/vedranvuk/flatfile"
)

func run(ff FlatFileInterface) time.Duration {
	locktestoptions := NewLockTestOptions()
	locktestoptions.Verbose = true
	locktestoptions.TestDuration = 10 * time.Second
	locktestoptions.MinGetDelay = 500
	locktestoptions.MaxGetDelay = 1000
	locktestoptions.MinPutDelay = 500
	locktestoptions.MaxPutDelay = 1000
	locktestoptions.MinDelDelay = 1000
	locktestoptions.MaxDelDelay = 2000
	locktestoptions.MinModDelay = 1000
	locktestoptions.MaxModDelay = 2000
	locktestoptions.MaxActiveR = 10
	locktestoptions.MaxActiveW = 1
	locktestoptions.MaxActiveD = 10
	locktestoptions.MaxActiveM = 10
	locktestoptions.MaxR = 10
	locktestoptions.MaxW = 10
	locktestoptions.MaxD = 0
	locktestoptions.MaxM = 0
	locktestoptions.QueueSizeR = 10
	locktestoptions.QueueSizeW = 10
	locktestoptions.QueueSizeD = 10
	locktestoptions.QueueSizeM = 10
	locktest := NewLockTest(locktestoptions)
	return locktest.Run(ff)
}

func RunEmu() (dur time.Duration) {
	log.Println("Running emu...")
	ffemu := NewFlatFileEmulator()
	dur = run(ffemu)
	return
}

func RunForReal() (dur time.Duration) {
	log.Println("Running for real...")

	if err := os.RemoveAll("testfile"); err != nil {
		log.Fatalf("Can't clear test data: %v", err)
	}

	options := flatfile.NewOptions()
	options.Immutable = false
	options.SyncWrites = false
	options.PersistentHeader = true
	options.MaxPageSize = 1048576 // 1MB

	ff, err := flatfile.Open("testfile", options)
	if err != nil {
		log.Fatal("Open error:", err)
	}

	dur = run(ff)

	if ff.Close(); err != nil {
		log.Fatalf("FATAL: Close: %v\n", err)
	}

	return dur
}

func main() {
	cputracef, err := os.Create("tester.pprof")
	if err != nil {
		panic(err)
	}
	defer func() {
		pprof.StopCPUProfile()
		cputracef.Close()
	}()
	pprof.StartCPUProfile(cputracef)

	memtracef, err := os.Create("tester.mprof")
	if err != nil {
		panic(err)
	}
	defer func() {
		memtracef.Close()
	}()

	emu := RunEmu()
	// emu := time.Duration(0)
	rly := RunForReal()
	pprof.WriteHeapProfile(memtracef)
	for i := 0; i < 5; i++ {
	}

	log.Println("---------------------------------------------")
	log.Printf("Emu took %s, Rly took %s\n", emu, rly)

}

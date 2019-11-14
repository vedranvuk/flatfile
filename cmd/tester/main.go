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
	locktestoptions.TestDuration = 10 * time.Second
	locktestoptions.MinGetDelay = 0
	locktestoptions.MaxGetDelay = 0
	locktestoptions.MinPutDelay = 0
	locktestoptions.MaxPutDelay = 0
	locktestoptions.MinDelDelay = 1
	locktestoptions.MaxDelDelay = 1
	locktestoptions.MinModDelay = 2
	locktestoptions.MaxModDelay = 2
	locktestoptions.MaxActiveR = 10
	locktestoptions.MaxActiveW = 1
	locktestoptions.MaxActiveD = 1
	locktestoptions.MaxActiveM = 1
	locktestoptions.MaxR = 10000
	locktestoptions.MaxW = 10000
	locktestoptions.MaxD = 10000
	locktestoptions.MaxM = 10000
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
	options.ZeroPadDeleted = false
	options.CachedWrites = true

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
	rly := RunForReal()
	log.Println("---------------------------------------------")
	log.Printf("Emu took %s, Rly took %s\n", emu, rly)
}

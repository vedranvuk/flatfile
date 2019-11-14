// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"log"
	"math/rand"
	"time"

	"github.com/vedranvuk/flatfile"
)

type LockTestOptions struct {
	Verbose      bool
	TestDuration time.Duration

	MinGetDelay time.Duration
	MaxGetDelay time.Duration
	MinPutDelay time.Duration
	MaxPutDelay time.Duration
	MinDelDelay time.Duration
	MaxDelDelay time.Duration
	MinModDelay time.Duration
	MaxModDelay time.Duration

	MaxActiveR int
	MaxActiveW int
	MaxActiveD int
	MaxActiveM int

	QueueSizeR int
	QueueSizeW int
	QueueSizeD int
	QueueSizeM int

	MaxR int
	MaxW int
	MaxD int
	MaxM int
}

func NewLockTestOptions() *LockTestOptions {
	o := &LockTestOptions{}
	o.init()
	return o
}

func (o *LockTestOptions) init() {

	o.Verbose = true

	o.TestDuration = 10 * time.Second

	o.MinGetDelay = 1 * time.Millisecond
	o.MaxGetDelay = 2 * time.Millisecond
	o.MinPutDelay = 2 * time.Millisecond
	o.MaxPutDelay = 5 * time.Millisecond
	o.MinDelDelay = 500 * time.Millisecond
	o.MaxDelDelay = 1000 * time.Millisecond
	o.MinModDelay = 1000 * time.Millisecond
	o.MaxModDelay = 2000 * time.Millisecond

	o.MaxActiveR = 10
	o.MaxActiveW = 1
	o.MaxActiveD = 1
	o.MaxActiveM = 1

	o.QueueSizeR = 100
	o.QueueSizeW = 10
	o.QueueSizeD = 10
	o.QueueSizeM = 10

	o.MaxR = 1000
	o.MaxW = 1000
	o.MaxD = 1000
	o.MaxM = 1000
}

type Request struct {
	Id     int
	Key    string
	Val    string
	Issued time.Time
}

type RequestChan chan Request
type TimeChan chan time.Time

type LockTest struct {
	Mute    bool
	options *LockTestOptions
	reqR    RequestChan
	reqW    RequestChan
	reqD    RequestChan
	reqM    RequestChan
}

func NewLockTest(options *LockTestOptions) *LockTest {
	p := &LockTest{
		options: options,
		reqR:    make(RequestChan),
		reqW:    make(RequestChan),
		reqD:    make(RequestChan),
		reqM:    make(RequestChan),
	}
	return p
}

func (lt *LockTest) Println(args ...interface{}) {
	if lt.Mute {
		return
	}
	log.Println(args...)
}

func (lt *LockTest) Printf(format string, args ...interface{}) {
	if lt.Mute {
		return
	}
	log.Printf(format, args...)
}

func (lt *LockTest) Fatalf(format string, args ...interface{}) {
	if lt.Mute {
		return
	}
	log.Fatalf(format, args...)
}

func (lt LockTest) requester(ticker TimeChan, mindly, maxdly time.Duration) {
	for {
		ticker <- time.Now()
		if maxdly <= mindly {
			continue
		}

		if dif := maxdly.Nanoseconds() - mindly.Nanoseconds(); dif > 0 {
			d := time.Duration(mindly.Nanoseconds() + rand.Int63n(dif))
			time.Sleep(time.Duration(d))
		}
	}
}

func (lt *LockTest) dispenser() {

	var (
		reqTickR = make(TimeChan)
		reqTickW = make(TimeChan)
		reqTickD = make(TimeChan)
		reqTickM = make(TimeChan)
	)

	go lt.requester(reqTickR, lt.options.MinGetDelay, lt.options.MaxGetDelay)
	go lt.requester(reqTickW, lt.options.MinPutDelay, lt.options.MaxPutDelay)
	go lt.requester(reqTickD, lt.options.MinDelDelay, lt.options.MaxDelDelay)
	go lt.requester(reqTickM, lt.options.MinModDelay, lt.options.MaxModDelay)

	lastReqTickR := time.Now()
	lastReqTickW := time.Now()
	lastReqTickD := time.Now()
	lastReqTickM := time.Now()
	testdata := NewTestData(0)

	operationID := 0
	for {
		select {
		case now := <-reqTickR:
			lt.Printf("Dispenser: Tick, Get after %s", now.Sub(lastReqTickR).String())
			lastReqTickR = time.Now()
			key, val := testdata.Peek()
			if key == "" {
				lt.Println("No data yet.")
				continue
			}
			lt.reqR <- Request{operationID, key, val, time.Now()}
			lt.Println("Dispanser: Request: Get, sent.")
		case now := <-reqTickW:
			lt.Printf("Dispenser: Tick, Put after %s", now.Sub(lastReqTickW).String())
			lastReqTickW = time.Now()
			key := testdata.GenKey()
			val := testdata.GenVal()
			testdata.Push(key, val)
			lt.reqW <- Request{operationID, key, val, time.Now()}
			lt.Println("Dispanser: Request: Put, sent.")
		case now := <-reqTickD:
			lt.Printf("Dispenser: Tick, Del after %s", now.Sub(lastReqTickD).String())
			lastReqTickD = time.Now()
			key, val := testdata.Pop()
			if key == "" {
				lt.Println("No data yet.")
				continue
			}
			lt.reqD <- Request{operationID, key, val, time.Now()}
			lt.Println("Dispanser: Request: Del, sent.")
		case now := <-reqTickM:
			lt.Printf("Dispenser: Tick, Mod after %s", now.Sub(lastReqTickM).String())
			lastReqTickM = time.Now()
			key, val := testdata.Pop()
			if key == "" {
				lt.Println("No data yet.")
				continue
			}
			val = testdata.GenVal()
			testdata.Push(key, val)
			lt.reqM <- Request{operationID, key, val, time.Now()}
			lt.Println("Dispanser: Request: Mod, sent.")
		}
		operationID++
	}
}

func (lt *LockTest) scheduler(ff FlatFileInterface, stop, done chan bool) {

	activeR := 0
	activeW := 0
	activeD := 0
	activeM := 0

	totalR := float64(0)
	totalW := float64(0)
	totalD := float64(0)
	totalM := float64(0)

	doneR := make(RequestChan)
	doneW := make(RequestChan)
	doneD := make(RequestChan)
	doneM := make(RequestChan)

	jobR := func(r *Request, done RequestChan) {
		lt.Printf("JobR: Key: %s,\n", r.Key)
		data, err := ff.Get([]byte(r.Key))
		if err != nil {
			if err == flatfile.ErrKeyNotFound {
				lt.Printf("jobR: Miss: '%s'\n", r.Key)
			} else {
				lt.Printf("FATAL: jobR: %v\n", err)
			}
		} else {
			if string(data) != r.Val && false {
				lt.Printf("FATAL: Get '%s' missmatch: need %s, got %s\n",
					r.Key, r.Val, string(data))
			}
		}
		dr := Request{}
		dr = *r
		dr.Issued = time.Now()
		done <- dr
	}

	jobW := func(r *Request, done RequestChan) {
		lt.Printf("JobW: Key: %s\n", r.Key)
		if err := ff.Put([]byte(r.Key), []byte(r.Val)); err != nil {
			lt.Fatalf("FATAL: jobW: %v\n", err)
		}
		dr := Request{}
		dr = *r
		dr.Issued = time.Now()
		done <- dr
	}

	jobD := func(r *Request, done RequestChan) {
		lt.Printf("JobD: Key: %s\n", r.Key)
		if err := ff.Delete([]byte(r.Key)); err != nil {
			if err == flatfile.ErrKeyNotFound {
				lt.Printf("jobD: Miss: '%s'\n", err)
			} else {
				lt.Printf("FATAL: jobD: %v\n", err)
			}
		}
		dr := Request{}
		dr = *r
		dr.Issued = time.Now()
		done <- dr
	}

	jobM := func(r *Request, done RequestChan) {
		lt.Printf("JobM: Key: %s\n", r.Key)
		if err := ff.Modify([]byte(r.Key), []byte(r.Val)); err != nil {
			if err == flatfile.ErrKeyNotFound {
				lt.Printf("jobM: Miss: '%s'\n", err)
			} else {
				lt.Printf("FATAL: jobM: %v\n", err)
			}
		}
		dr := Request{}
		dr = *r
		dr.Issued = time.Now()
		done <- dr
	}

	dlog := func(typ, op string, r *Request) {
		lt.Printf("Scheduler: (%6.d) %s: %s '%s' after %s", r.Id, typ, op, r.Key, time.Since(r.Issued))
	}

	queueR := []*Request{}
	queueW := []*Request{}
	queueD := []*Request{}
	queueM := []*Request{}

	start := time.Now()
	run := true
	for func(bool) bool {
		if run && lt.options.MaxR > 0 && int(totalR) >= lt.options.MaxR &&
			lt.options.MaxW > 0 && int(totalW) >= lt.options.MaxW &&
			lt.options.MaxD > 0 && int(totalD) >= lt.options.MaxD &&
			lt.options.MaxM > 0 && int(totalM) >= lt.options.MaxM {
			run = false
			lt.Println("Max operations reached.")
		}
		if !run {
			if activeR+activeW+activeD+activeM > 0 {
				lt.Printf(`Waiting for operations to complete:
R: %d
W: %d
D: %d
M: %d`, activeR, activeW, activeD, activeM)
				return true
			}
			return false
		}
		if len(queueW) > 0 && int(totalW) < lt.options.MaxW {
			if activeW >= lt.options.MaxActiveW {
				return true
			}
			if activeR > 0 {
				return true
			}
			activeW++
			go jobW(queueW[0], doneW)
			queueW = queueW[1:]
			return true
		}
		if len(queueD) > 0 && int(totalD) < lt.options.MaxD {
			if activeD >= lt.options.MaxActiveD {
				return true
			}
			if activeR > 0 {
				return true
			}
			activeD++
			go jobD(queueD[0], doneD)
			queueD = queueD[1:]
			return true
		}
		if len(queueM) > 0 && int(totalM) < lt.options.MaxM {
			if activeM >= lt.options.MaxActiveM {
				return true
			}
			if activeR > 0 {
				return true
			}
			activeM++
			go jobM(queueM[0], doneM)
			queueM = queueM[1:]
			return true
		}
		if len(queueR) > 0 && int(totalR) < lt.options.MaxR {
			if activeR >= lt.options.MaxActiveR {
				return true
			}
			if activeR > 0 {
				return true
			}
			activeR++
			go jobR(queueR[0], doneR)
			queueR = queueR[1:]
			return true
		}

		return true
	}(run) {
		select {

		case r := <-lt.reqR:
			dlog("Request", "Get", &r)
			queueR = append(queueR, &r)
			lt.Printf("RQ: %+v\n", queueR)
		case d := <-doneR:
			dlog("Complete", "Get", &d)
			activeR--
			totalR++

		case r := <-lt.reqW:
			dlog("Request", "Put", &r)
			queueW = append(queueW, &r)
			lt.Printf("RW: %+v\n", queueW)
		case d := <-doneW:
			dlog("Complete", "Put", &d)
			activeW--
			totalW++

		case r := <-lt.reqD:
			dlog("Request", "Del", &r)
			queueD = append(queueD, &r)
			lt.Printf("RD: %+v\n", queueD)
		case d := <-doneD:
			dlog("Complete", "Del", &d)
			activeD--
			totalD++

		case r := <-lt.reqM:
			dlog("Request", "Mod", &r)
			queueM = append(queueM, &r)
			lt.Printf("RM: %+v\n", queueM)
		case d := <-doneM:
			dlog("Complete", "Mod", &d)
			activeM--
			totalM++

		case <-stop:
			lt.Println("Stop requested...")
			run = false
		}
	}
	lt.Mute = false
	lt.Printf(`LockTest completed:
Totals:
  Total R: %.f
  Total W: %.f
  Total D: %.f
  Total M: %.f
  R/s:     %.2f
  W/s:     %.2f
  D/s:     %.2f
  M/s:     %.2f`,
		totalR, totalW, totalD, totalM,
		totalR*time.Second.Seconds()/time.Since(start).Seconds(),
		totalW*time.Second.Seconds()/time.Since(start).Seconds(),
		totalD*time.Second.Seconds()/time.Since(start).Seconds(),
		totalM*time.Second.Seconds()/time.Since(start).Seconds(),
	)
	lt.Println()
	done <- true
}

func (lt *LockTest) Run(ff FlatFileInterface) time.Duration {

	if !lt.options.Verbose {
		lt.Mute = true
	}

	start := time.Now()
	stop := make(chan bool)
	done := make(chan bool)

	go lt.dispenser()
	go lt.scheduler(ff, stop, done)

	limit := time.After(lt.options.TestDuration)
	select {
	case <-limit:
		lt.Println("LockTest time limit reached.")
		stop <- true
	case <-done:
		break
	}
	return time.Since(start)
}

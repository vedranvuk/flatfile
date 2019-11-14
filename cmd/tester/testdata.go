// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"math/rand"

	"github.com/vedranvuk/strings"
)

// TestPair
type TestPair struct {
	key string
	val string
}

// TestData
type TestData struct {
	pairs []*TestPair
	keys  map[string]bool
	vals  map[string]bool
}

// Returns new test data.
func NewTestData(entrycount int) *TestData {
	p := &TestData{
		pairs: make([]*TestPair, entrycount),
		keys:  make(map[string]bool),
		vals:  make(map[string]bool),
	}
	for i := 0; i < len(p.pairs); i++ {
		p.pairs[i] = &TestPair{
			key: strings.RandomString(true, true, true, 8+rand.Intn(8)),
			val: strings.RandomString(true, true, true, 8+rand.Intn(1024)),
		}
		p.keys[p.pairs[i].key] = true
		p.vals[p.pairs[i].val] = true
	}
	return p
}

// generates a random key
func (td *TestData) GenKey() string {
	for {
		key := strings.RandomString(true, true, true, 8+rand.Intn(8))
		if _, ok := td.keys[key]; ok {
			continue
		}
		return key
	}
}

// generates a random value
func (td *TestData) GenVal() string {
	for {
		val := strings.RandomString(true, true, true, 8+rand.Intn(1024))
		if _, ok := td.vals[val]; ok {
			continue
		}
		return val
	}
}

// Push adds an entry.
func (td *TestData) Push(key, val string) {
	td.pairs = append(td.pairs, &TestPair{key, val})
}

// Peek returns a random entry without deleting it.
func (td *TestData) Peek() (key, val string) {
	if len(td.pairs) == 0 {
		return
	}
	i := rand.Intn(len(td.pairs))
	key = td.pairs[i].key
	val = td.pairs[i].val
	return
}

// Pop returns a random entry and deletes it.
func (td *TestData) Pop() (key, val string) {
	if len(td.pairs) == 0 {
		return
	}
	i := rand.Intn(len(td.pairs))
	key = td.pairs[i].key
	val = td.pairs[i].val

	if i < len(td.pairs)-1 {
		copy(td.pairs[i:], td.pairs[i+1:])
	}
	td.pairs[len(td.pairs)-1] = nil
	td.pairs = td.pairs[:len(td.pairs)-1]
	delete(td.keys, key)
	delete(td.vals, val)
	return
}

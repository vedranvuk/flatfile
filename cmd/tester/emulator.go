// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

import (
	"sync"

	"github.com/vedranvuk/flatfile"
)

type FlatFileEmulator struct {
	m     sync.RWMutex
	items map[string]string
}

func NewFlatFileEmulator() *FlatFileEmulator {
	return &FlatFileEmulator{
		m:     sync.RWMutex{},
		items: make(map[string]string),
	}
}

func (ffe *FlatFileEmulator) Put(key, val []byte) error {
	ffe.m.Lock()
	defer ffe.m.Unlock()

	if len(key) == 0 {
		return flatfile.ErrInvalidKey
	}

	if _, ok := ffe.items[string(key)]; ok {
		return nil
		return flatfile.ErrDuplicateKey
	}

	ffe.items[string(key)] = string(val)
	return nil
}

func (ffe *FlatFileEmulator) Get(key []byte) ([]byte, error) {
	ffe.m.RLock()
	defer ffe.m.RUnlock()

	v, ok := ffe.items[string(key)]
	if !ok {
		return nil, flatfile.ErrKeyNotFound
	}

	return []byte(v), nil
}

func (ffe *FlatFileEmulator) Delete(key []byte) error {
	ffe.m.Lock()
	defer ffe.m.Unlock()

	delete(ffe.items, string(key))
	return nil
}

func (ffe *FlatFileEmulator) Modify(key, val []byte) error {
	ffe.m.Lock()
	defer ffe.m.Unlock()

	ffe.items[string(key)] = string(val)
	return nil

}

// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package main

type FlatFileInterface interface {
	Put(key, val []byte) error
	Get(key []byte) ([]byte, error)
	Delete([]byte) error
	Modify(key, val []byte) error
}

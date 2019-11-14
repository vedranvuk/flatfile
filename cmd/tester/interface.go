package main

type FlatFileInterface interface {
	Put(key, val []byte) error
	Get(key []byte) ([]byte, error)
	Delete([]byte) error
	Modify(key, val []byte) error
}

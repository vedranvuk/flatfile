# flatfile	

Package flatfile implements a disk datastore with a key/value interface. 

The motivation was to build a simple, small, fast and above all straightforward and easy to use no-delete datastore, but functionality to modify data has been added as well, albeit with drawbacks.

## Status

Few features and an exhaustive test left to implement.

## Features

* Get, Put, Delete and Modify operations.
* Safe for concurrent use.
* Memory caching capability.
* Data safety and redundancy mechanisms.

## Implementation

On-disk FlatFile is a directory which holds a .header, .stream(s) and .options files. Header persists cells which define a data blob in a Stream. Stream holds the actual blobs. Options file persists FlatFile options between sessions. Stream can be multi-page and pre-allocated.

Put keys a data blob, writes it to Stream then marks it using a cell stored to Header. Get returns blob from a Stream by key. Delete marks a blob as deleted and ready for reuse. Modify issues a Delete then a Put.

Deleted blobs are reused by picking the blob that is closest and at least the size of Put at Put time. Rest of reused blob is empty until next possible reuse which can be more or less space efficient. If no deleted blobs can hold Put, a new blob is created. Blobs are always written in single chunks and don't span across pages. If there is a Page size limit, Put blob must be smaller than a page.

This approach gives fast, direct I/O but data modifications cause fragmentation during writes. To battle blob data fragmentation Stream pages can be preallocated. To minimize wasted space which results from zero-padding the unused space of reused cells a manual Concat function can re-create the Stream, at runtime or otherwise.

For redundancy, FlatFile can maintain an up-to-date mirror copy of itself in a separate location at runtime. To battle data loss caused by a power outage in the middle of a write with OS disk-write-caching enabled Intent files can be used during process of data modification.

## Interface

The main functionality of FlatFile is exposed in several methods.
```
	Open(filename string, options *FlatFileOptions) (*FlatFile, error)
	Get(key []byte) (val []byte, err error)
	Put(key, val []byte) error
	Modify(key, val []byte) error
	Delete(key []byte) error
	Walk(f func(key, val []byte) (resume bool))
	Close() error
```

## Pros

* Fast I/O.
* No Put size limit.
* Easy to use.

## Cons

* Waste space introduced with each Delete and Modify operation.
* Potentinal fragmentation increased with non-preallocated stream.

## License

MIT License. See included LICENSE file.



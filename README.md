# flatfile	

Package flatfile implements a disk datastore with a key/value interface and a Journal-like behaviour. 

The motivation was to build a simple, small, easy to use and relatively fast datastore.

## Features

* Get, Put, Delete and Modify operations.
* Safe for concurrent use.
* In-memory or persistent.
* Caching.
* Data safety and redundancy mechanisms.
* On-the-fly data (de)encryption/encoding.

## Implementation

Data consists of a directory which holds a .header, .stream(s) and .options files. Header persists cells which define a data blob in a Stream. Stream holds the actual blobs. Options file persists FlatFile options between sessions. Stream can be multi-page and pre-allocated.

Put appends keyed data blobs to Stream and marks the location in the Header for fast retrieval. Get returns blob from a Stream by key. Delete marks a blob as deleted and ready for reuse. Modify issues a Delete then a Put.

Deleted blobs are reused by picking the blob that is closest and at least the size of Put at Put time. Rest of reused blob is empty until next possible reuse which can be more or less space efficient. If no deleted blobs can hold Put, a new blob is created. Blobs are always written in single chunks and don't span across pages. If there is a Page size limit, Put blob must be smaller than a page.

This approach gives fast IO but leads to fragmenting. To battle blob data fragmentation Stream pages can be preallocated. To minimize waste space which results from zero-padding the unused space of reused cells a manual Concat function can re-create the Header and Stream, at runtime or otherwise.

For redundancy, FlatFile can simultaneously maintain an up-to-date copy of itself in a separate location.

Blobs can - at the expense of performance - optionally be backed up before writes so a potential mid-write corruption can be remedied. Additional optional timestamping of blobs - at the expense of Header size - can help restore as much data as possible from a heavily corrupted FlatFile using the provided management tools.

Public interface to FlatFile:
```
type FlatFileInterface interface {
	Open(filename string, options *FlatFileOptions) (*FlatFile, error)
	Get(key []byte) (val []byte, err error)
	Put(key, val []byte) error
	Modify(key, val []byte) error
	Delete(key []byte) error
	Walk(f func(key, val []byte) (resume bool))
	Concat() error
	Close() error
}
```

### Pros

* Fast I/O.
* No Put size limit.
* Easy to use.
* Data safety (at performance expense).

### Cons

* Increased file size.
* Possible data fragmentation (depending on configuration).



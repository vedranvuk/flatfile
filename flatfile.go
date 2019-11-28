// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

// Package flatfile implements a flat file disk storage. It is a simple,
// straightforward key/value store. It supports Get, Put, Modify and Delete.
//
// Intended for use as a standalone datastore or a backend to a more complex
// storage system. Performance wise, it holds quite OK. Data is immediately
// addressable so retrieval speed basically depends solely on storage
// hardware. The idea behind implementation was to sacrifice space for maximum
// speed and flexibility.
//
// Actual 'file' consists of .header, one or more .stream files and an .options
// file. Header holds a collection of cells which describe blobs inside a Stream.
// Stream holds the blobs and .options persist Options specified in first session.
//
// Header is packed, cell entries are of variable length and are (re)loaded,
// sequentially, once per session then always remain in memory during a session.
// Header serialization can be instant or once on session end. If Header
// serialization is instant, a new cell allocation or a modification of an
// existing cell is immediately written to Header. This increases data safety
// and reduces speed but results in Header having multiple records of a single
// unique cell in different states, as they changed, from start of Header file,
// towards its' end. By default, header is truncated to hold only unique cell
// entries each time it is (re)loaded, but can be set to hold the complete
// history of changes.
//
// Stream is always immediately persisted. Stream size can be limited and split
// across files as pages. In that case Put data size must be less than the page
// size limit. Pages can be preallocated. A new blob that doesn't fit in the
// leftover space in a page is stored in a new page and the previous page is
// left with empty space, if preallocated.
//
// Any changes to Stream not backed by cell entries in Header are lost and
// eventually possibly overwritten. For example, if a power outage occurs during
// a session where Header is set to persist on session end and there were
// modifications to the file.
//
// Cells, when newly created, allocate space in the Stream of same size as the
// Put operation data that initiated it. As both Header and Stream are written
// sequentially cells and blobs can't be resized once allocated but blobs can
// be reused after they have been deleted.
//
// Deletes simply mark cells as deleted. Successive Puts will try and reuse
// deleted cells if a deleted cell with allocated blob space which is bigger
// and as close as possible to Put data size is found. If there are no such
// cells a new one is created.
//
// FlatFile can be Compacted to trim unused space both from Header and Stream.
package flatfile

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	HeaderExt  = "header"
	StreamExt  = "stream"
	OptionsExt = "options"
	IntentExt  = "intent"
	ConcatExt  = "concat"
)

// FlatFile represents the actual flat file.
type FlatFile struct {
	mutex   sync.RWMutex
	options *Options
	header  *header
	stream  *stream
	mirror  *FlatFile
}

// Open opens an existing or creates a new FlatFile in the
// base directory of filename. Close() MUST be called after
// use to free resources and open file descriptors.
func Open(filename string, options *Options) (*FlatFile, error) {

	// Extract FlatFile name from the base of the specified filename.
	bn := filepath.Base(filename)
	if bn == "." || bn == "/" {
		return nil, ErrFlatFile.Errorf("invalid filename: '%s'", filename)
	}
	// Check if FlatFile dir already exists and if not, create it.
	dirExists, err := FileExists(filename)
	if err != nil {
		return nil, ErrFlatFile.Errorf("base dir '%s' stat error: %w", filename, err)
	}
	if !dirExists {
		if err := os.MkdirAll(filename, os.ModePerm); err != nil {
			return nil, ErrFlatFile.Errorf("can't create base dir '%s': %w", filename, err)
		}
	}
	// Create a FlatFile.
	ff := &FlatFile{
		mutex:   sync.RWMutex{},
		options: options,
		header:  newHeader(fmt.Sprintf("%s.%s", filepath.Join(filename, bn), HeaderExt)),
		stream:  newStream(filepath.Join(filename, bn)),
	}
	// load options.
	if ff.options == nil {
		ff.options = NewOptions()
	}
	ff.options.filename = fmt.Sprintf("%s.%s", filepath.Join(filename, bn), OptionsExt)
	if err := ff.loadOptions(); err != nil {
		return nil, err
	}
	// Load file.
	if err := ff.load(ff.options.CompactHeader); err != nil {
		return nil, err
	}
	// Check intents.
	// TODO Check intents.
	// Setup optional mirror.
	if ff.options.MirrorDir != "" && !ff.options.mirrored {
		mirroropt := NewOptions()
		*mirroropt = *ff.options
		mirroropt.mirrored = true
		mirror, err := Open(ff.options.MirrorDir, mirroropt)
		if err != nil {
			return nil, ErrFlatFile.Errorf("mirror error: %w", err)
		}
		ff.mirror = mirror
	}
	return ff, nil
}

// loadOptions loads options, if they exist.
func (ff *FlatFile) loadOptions() error {
	exists, err := FileExists(ff.options.filename)
	if err != nil {
		return ErrFlatFile.Errorf("options stat error: %w", err)
	}
	if !exists {
		return nil
	}
	file, err := os.OpenFile(ff.options.filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return ErrFlatFile.Errorf("options open error: %w", err)
	}
	defer file.Close()
	return ff.options.Unmarshal(file)
}

// saveOptions saves options owerwriting existing file.
func (ff *FlatFile) saveOptions() (err error) {
	file, err := os.OpenFile(
		ff.options.filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return ErrFlatFile.Errorf("options create error: %w", err)
	}
	defer file.Close()
	err = ff.options.Marshal(file)
	return
}

// load loads the Header and Stream.
func (ff *FlatFile) load(compactheader bool) (err error) {
	// Open and load the header.
	maxpage, err := ff.header.Open(ff.options.CompactHeader, ff.options.SyncWrites)
	if err != nil {
		return ErrFlatFile.Errorf("header open error: %w", err)
	}
	// No keys.
	if ff.Len() == 0 {
		return
	}
	// Open stream page files.
	if err = ff.stream.Open(maxpage+1, ff.options.SyncWrites); err != nil {
		ff.header.Close()
		return ErrFlatFile.Errorf("stream open error: %w", err)
	}
	return
}

// Close closes the FlatFile.
func (ff *FlatFile) Close() (err error) {
	erro := ff.saveOptions()
	errh := ff.header.Close()
	errs := ff.stream.Close()
	errm := error(nil)
	if ff.mirror != nil {
		errm = ff.mirror.Close()
	}
	if erro != nil || errh != nil || errs != nil || errm != nil {
		return ErrFlatFile.Errorf(`close errors: 
	options: %v
	header:  %v
	stream:  %v
	mirror:  %v`,
			erro, errh, errs, errm)
	}
	return nil
}

// Reopen closes and reopens header and stream.
func (ff *FlatFile) Reopen() (err error) {
	if err = ff.Close(); err != nil {
		return
	}
	if err = ff.load(ff.options.CompactHeader); err != nil {
		return
	}
	if ff.mirror != nil {
		if err = ff.mirror.Reopen(); err != nil {
			return ErrFlatFile.Errorf("mirror error: %w", err)
		}
	}
	return
}

// Walk walks the FlatFile by calling f with currently enumerated key/value
// pair as parameters. f should return true to continue enumeration.
func (ff *FlatFile) Walk(f func(key, val []byte) bool) error {

	ff.mutex.Lock()
	defer ff.mutex.Unlock()

	for k := range ff.header.keys {
		data, err := ff.get([]byte(k), true)
		if err != nil {
			if err == ErrKeyNotFound {
				continue
			}
			return err
		}
		if !f([]byte(k), data) {
			break
		}
	}
	return nil
}

// Compact compacts header and stream into a temp file then rotates them with
// main files. Writes are locked during Concat. Returns an error if one occurs.
func (ff *FlatFile) Compact() error {
	// TODO: Implement Compact().

	ff.mutex.RLock()
	defer ff.mutex.RUnlock()

	return nil
}

// Len returns number of keys.
func (ff *FlatFile) Len() int {

	ff.mutex.RLock()
	defer ff.mutex.RUnlock()

	return len(ff.header.keys)
}

// put is the Put implementation.
// If a put fails mid-write, any data that is partially written will be
// overwritten on next Put.
func (ff *FlatFile) put(key, val []byte) error {
	// undoputcell undoes states made for putcell.
	// Mid-op error cleanup.
	undoputcell := func(c *cell) {
		switch c.CellState {
		case StateNormal:
			ff.header.Destroy(c)
		default:
			ff.header.UnCache(c)
			c.CRC32 = 0
			ff.header.Trash(c)
		}
	}
	// Check key validity.
	if len(key) == 0 {
		return ErrInvalidKey
	}
	// Check if key is in use.
	if ff.header.IsKeyUsed(string(key)) {
		return ErrDuplicateKey
	}
	// Check if data is bigger than page size.
	putsize := len(val)
	if ff.options.MaxPageSize > 0 && int64(putsize) > ff.options.MaxPageSize {
		return ErrBlobTooBig
	}
	// Initialize a cell.
	putcell := ff.header.Select(!ff.options.Immutable, int64(putsize))
	putcell.key = string(key)
	// Generate blob checksum.
	if ff.options.CRC {
		putcell.CRC32 = crc32.ChecksumIEEE(val)
	}
	// Cache cell if requested.
	if ff.options.MaxCacheMemory > 0 && ff.options.CachedWrites && !ff.options.mirrored {
		ff.header.Cache(putcell, val, ff.options.MaxCacheMemory)
	}
	// Get page.
	putpage, err := ff.stream.GetCellPage(
		putcell,
		ff.options.MaxPageSize,
		ff.options.PreallocatePages,
		ff.options.SyncWrites)
	if err != nil {
		undoputcell(putcell)
		return ErrFlatFile.Errorf("page alloc error: %w", err)
	}
	// Write blob.
	if err := putpage.Put(putcell, val, ff.options.ZeroPadDeleted); err != nil {
		undoputcell(putcell)
		return ErrFlatFile.Errorf("put error: %w", err)
	}
	// Update header file.
	if err := ff.header.Update(putcell, ff.options.PersistentHeader); err != nil {
		undoputcell(putcell)
		return ErrFlatFile.Errorf("put error: %w", err)
	}
	// Append the cell.
	ff.header.Use(putcell)
	return nil
}

// Put puts val into FlatFile under key or returns an error if one occurs.
func (ff *FlatFile) Put(key, val []byte) error {

	ff.mutex.Lock()
	defer ff.mutex.Unlock()

	if err := ff.put(key, val); err != nil {
		return err
	}
	if ff.mirror != nil {
		if err := ff.mirror.Put(key, val); err != nil {
			return ErrFlatFile.Errorf("mirror error: %w", err)
		}
	}
	return nil
}

// get is the Get implementation.
func (ff *FlatFile) get(key []byte, walking bool) (blob []byte, err error) {
	// Check key.
	if len(key) == 0 {
		return nil, ErrInvalidKey
	}
	cell, ok := ff.header.keys[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	// Retrieve blob.
	if len(cell.cache) == 0 {
		page := ff.stream.pages[cell.PageIndex]
		blob = make([]byte, cell.Used)
		blob, err = page.Get(cell)
		if err != nil {
			return nil, ErrFlatFile.Errorf("get error: %w", err)
		}
		if ff.options.CRC && cell.CRC32 != 0 {
			crc := crc32.ChecksumIEEE(blob)
			if crc != cell.CRC32 {
				return nil, ErrChecksumFailed
			}
		}
	} else {
		blob = cell.cache
	}
	// Cache cell if requested.
	if ff.options.MaxCacheMemory > 0 && !walking {
		if cell.cache == nil {
			cell.cache = blob
		}
		ff.header.Cache(cell, blob, ff.options.MaxCacheMemory)
	}
	return
}

// Get gets data from FlatFile with the specified unique id. If an error occurs
// it is returned.
func (ff *FlatFile) Get(key []byte) (blob []byte, err error) {

	ff.mutex.RLock()
	defer ff.mutex.RUnlock()

	return ff.get(key, false)
}

// GetR returns a LimitedReadSeekCloser bounded to cell blob.
// Caller should Close() the LimitedReadSeekCloser after use.
func (ff *FlatFile) GetR(key []byte) (r io.ReadSeeker, err error) {

	ff.mutex.RLock()
	defer ff.mutex.RUnlock()

	cell, ok := ff.header.keys[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	if cell.CellState == StateDeleted {
		return nil, ErrKeyNotFound
	}
	if len(cell.cache) == 0 {
		fn := ff.stream.pages[cell.PageIndex].filename
		file, err := os.OpenFile(fn, os.O_RDONLY, os.ModePerm)
		if err != nil {
			return nil, err
		}
		return NewLimitedReadSeekCloser(file, cell.Offset, cell.Allocated)
	} else {
		return bytes.NewReader(cell.cache), nil
	}
}

// Modify modifies an existing blob specified under key by replacing it with
// specified val. If an error occurs it is returned.
func (ff *FlatFile) Modify(key, val []byte) (err error) {

	if ff.options.Immutable {
		return ErrImmutableFile
	}
	if len(key) == 0 {
		return ErrInvalidKey
	}

	ff.mutex.Lock()
	defer ff.mutex.Unlock()

	if _, ok := ff.header.keys[string(key)]; !ok {
		return ErrKeyNotFound
	}
	if ff.options.MaxPageSize > 0 && int64(len(val)) > ff.options.MaxPageSize {
		return ErrBlobTooBig
	}
	// TODO Implement intent.
	if err = ff.delete(key); err != nil {
		return
	}
	if err := ff.put(key, val); err != nil {
		return err
	}
	if ff.mirror != nil {
		if err := ff.mirror.Modify(key, val); err != nil {
			return ErrFlatFile.Errorf("mirror error: %w", err)
		}
	}
	return nil
}

// delete is Delete implementation.
func (ff *FlatFile) delete(key []byte) error {

	k := string(key)

	cell, ok := ff.header.keys[k]
	if !ok {
		return ErrKeyNotFound
	}
	if cell.CellState == StateDeleted {
		return nil
	}
	delete(ff.header.keys, k)
	ff.header.UnCache(cell)
	ff.header.Trash(cell)
	cell.key = ""
	cell.CRC32 = 0
	cell.CellState = StateDeleted

	return ff.header.Update(cell, ff.options.PersistentHeader)
}

// Delete marks a blob specified under key as deleted. If an error occurs it
// is returned.
func (ff *FlatFile) Delete(key []byte) error {

	if ff.options.Immutable {
		return ErrImmutableFile
	}
	if len(key) == 0 {
		return ErrInvalidKey
	}

	ff.mutex.Lock()
	defer ff.mutex.Unlock()

	if err := ff.delete(key); err != nil {
		return err
	}
	if ff.mirror != nil {
		if err := ff.mirror.Delete(key); err != nil {
			return ErrFlatFile.Errorf("mirror error: %w", err)
		}
	}
	return nil
}

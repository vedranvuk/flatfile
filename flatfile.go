// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

// Package flatfile implements a flat file disk storage. It is a simple,
// straightforward key/value store. It supports Get, Put, Modify and Delete.
//
// Using Open function which takes a filename of a possibly non-existant
// directory either creates a flatfile in the directory specified or opens the
// flatfile from the specified directory if it exists.
//
// Actual 'file' consists of .header, .stream and .options files.
// Header holds a collection of cells which describe blobs inside a Stream.
// .options persist Options specified in first session.
//
// Header is packed, cell entries are of variable length and are loaded once
// per session and remain in memory until saved and reloaded between sessions.
// Header persistence can be instant, once on session end, or manually.
// Stream is immediately persisted.
//
// Stream size can be limited and split across files as pages. In that case Put
// data size must be less than the page size limit. Pages can be preallocated.
// A new blob that doesn't fit in the leftover space in a page is stored in a
// new page and the previous page is left with empty space, if preallocated.
//
// Delete simply marks the cell as deleted. Successive Puts will reuse deleted
// cells if their allocated blob space is as bigger and as close as possible to
// Put data size. If there are no such cells a new one is created. Once
// allocated blob space cannot be resized but can be reused.
//
// Both the Header and Stream can be recreated manually to prune modified cells
// and pack the .header and .stream to smallest possible size.
//
// Changing options between sessions is not allowed via Open but can be changed
// if options are clear text and will not corrupt the file.
package flatfile

import (
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

const (
	HeaderExt  = "header"
	StreamExt  = "stream"
	OptionsExt = "options"
	ConcatExt  = "concat"
)

var (
	// ErrImmutableFile is returned when a Modify or Delete method has been
	// called on a file that is opened as immutable.
	ErrImmutableFile = errors.New("immutable file")

	// ErrBlobToBig is returned in a Put or Modify operation when data size
	// exceeds Options.MaxPageSize.
	ErrBlobTooBig = errors.New("blob too big")

	// ErrKeNotFound is returned when a blob under specified key is not found.
	ErrKeyNotFound = errors.New("key not found")

	// ErrDuplicateKey is returned if a key already exists during Put.
	ErrDuplicateKey = errors.New("duplicate key")

	// ErrInvalidKey is returned when an invalid key was specified.
	ErrInvalidKey = errors.New("invalid key")

	ErrChecksumFailed = errors.New("blob checksum failed")
)

// FlatFile represents the actual flat file.
type FlatFile struct {
	mutex   sync.RWMutex
	options *Options
	header  *header
	stream  *stream
	mirror  *FlatFile
}

// Open opens an existing or creates a new flatfile. filename is a name of a
// directory where header and stream files consisting flatfile are located.
// Close() should be called after use to free the file descriptors.
func Open(filename string, options *Options) (*FlatFile, error) {

	// Extract FlatFile name from the base of the specified filename.
	bn := filepath.Base(filename)
	if bn == "." || bn == "/" {
		return nil, fmt.Errorf("invalid filename: '%s'", filename)
	}
	// Check if FlatFile dir already exists and if not, create it.
	dirExists, err := FileExists(filename)
	if err != nil {
		return nil, fmt.Errorf(
			"flatfile dir stat '%s' error: %w", filename, err)
	}
	if !dirExists {
		if err := os.MkdirAll(filename, os.ModePerm); err != nil {
			return nil, fmt.Errorf("can't create flatfile dir: %w", err)
		}
	}
	// Create a FlatFile
	ff := &FlatFile{
		mutex:   sync.RWMutex{},
		options: options,
		stream:  &stream{},
	}
	if ff.options == nil {
		ff.options = NewOptions()
	}
	// Load options
	ff.options.filename = filepath.Join(filename, bn+"."+OptionsExt)
	if err := ff.loadOptions(); err != nil {
		return nil, fmt.Errorf("options load error: %w", err)
	}
	// Create header and stream.
	ff.stream = newStream(filepath.Join(filename, bn))
	ff.header = newHeader(filepath.Join(filename, bn+"."+HeaderExt))
	// Set up mirror file.
	if options.MirrorDir != "" && !options.mirror {
		mirroroptions := *ff.options
		mirroroptions.mirror = true
		mn := filepath.Join(options.MirrorDir, bn)
		mf, err := Open(mn, &mirroroptions)
		if err != nil {
			return nil, fmt.Errorf("error setting up mirror file: %w", err)
		}
		ff.mirror = mf
	}
	// Open and load the header.
	if err = ff.header.OpenOrCreate(ff.options.SyncWrites); err != nil {
		return nil, fmt.Errorf("error opening header: %w", err)
	}
	if err = ff.header.LoadCells(); err != nil {
		return nil, fmt.Errorf("error loading header: %w", err)
	}
	// Open stream page files.
	if ff.Len() > 0 {
		if err = ff.stream.Open(
			ff.header.CurrentPageIndex()+1, ff.options.SyncWrites); err != nil {

			ff.header.Close()
			return nil, fmt.Errorf("error opening stream: %w", err)
		}
	}
	return ff, nil
}

// loadOptions loads options if they exist.
func (ff *FlatFile) loadOptions() error {
	exists, err := FileExists(ff.options.filename)
	if err != nil {
		return fmt.Errorf("options stat error: %w", err)
	}
	if !exists {
		return nil
	}
	file, err := os.OpenFile(ff.options.filename, os.O_RDONLY, os.ModePerm)
	if err != nil {
		return err
	}
	defer file.Close()
	opt := NewOptions()
	opt.filename = ff.options.filename
	ff.options = opt
	err = ff.options.Unmarshal(file)
	return nil
}

// saveOptions saves options.
func (ff *FlatFile) saveOptions() (err error) {
	file, err := os.OpenFile(
		ff.options.filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return fmt.Errorf("options save error: %w", err)
	}
	defer file.Close()
	err = ff.options.Marshal(file)
	return
}

// Close closes the FlatFile.
func (ff *FlatFile) Close() error {
	erro := ff.saveOptions()
	errh := ff.header.Close()
	errs := ff.stream.Close()
	var errm error
	if ff.mirror != nil {
		errm = ff.mirror.Close()
	}
	if erro != nil || errh != nil || errs != nil || errm != nil {
		return fmt.Errorf(
			"error closing flatfile: options: %s, header %s, stream %s, mirror: %s",
			erro, errh, errs, errm)
	}
	return nil
}

// Len returns number of blobs in the file.
func (ff *FlatFile) Len() int {

	ff.mutex.RLock()
	defer ff.mutex.RUnlock()

	return len(ff.header.cells) - len(ff.header.deletedCells.cells)
}

// put is the Put implementation.
// If a put fails mid-write, any data that is partially written will be
// overwritten on next Put.
func (ff *FlatFile) put(key, val []byte) error {
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
	putcell := ff.header.MakeCell(!ff.options.Immutable, int64(putsize))
	// Generate blob checksum.
	if ff.options.CRC {
		putcell.CRC32 = crc32.ChecksumIEEE(val)
	}
	// Cache cell if requested.
	if ff.options.CachedWrites && ff.options.MaxCacheMemory > 0 && !ff.options.mirror {
		putcell = ff.header.CacheCell(putcell, key, val, ff.options.MaxCacheMemory)
	}
	// Get page.
	putcell, putpage, err := ff.stream.GetCellPage(putcell, ff.options.MaxPageSize, ff.options.PreallocatePages)
	if err != nil {
		return fmt.Errorf("page alloc error: %w", err)
	}
	// Seek then Write blob.
	if _, err := putpage.file.Seek(int64(putcell.Offset), os.SEEK_SET); err != nil {
		return fmt.Errorf("stream seek error: %w", err)
	}
	if _, err := putpage.file.Write(val); err != nil {
		return fmt.Errorf("stream write error: %w", err)
	}
	// Fill the rest with 0s, if requested.
	if !ff.options.Immutable && ff.options.ZeroPadDeleted && putcell.CellState != StateNormal {
		buf := make([]byte, putcell.Allocated-putcell.Used)
		if _, err := putpage.file.Write(buf); err != nil {
			return fmt.Errorf("stream write error: %w", err)
		}
	}
	// Sync if requested.
	if ff.options.SyncWrites {
		if err := putpage.file.Sync(); err != nil {
			return fmt.Errorf("stream sync failed: %w", err)
		}
	}
	// If SyncHeader is enabled, seek to header end, write length of binary
	// encoded cell then the cell itself, otherwise mark cell dirty.
	if ff.options.PersistentHeader {
		if _, err := ff.header.file.Seek(0, os.SEEK_END); err != nil {
			return fmt.Errorf("header seek error: %w", err)
		}
		if err := putcell.write(ff.header.file, string(key)); err != nil {
			return err
		}
		if ff.options.SyncWrites {
			if err := ff.header.file.Sync(); err != nil {
				return fmt.Errorf("header sync failed: %w", err)
			}
		}
	} else {
		ff.header.MarkCellDirty((string(key)))
	}
	// Append the cell.
	ff.header.AddCell(string(key), putcell)
	return nil
}

// Put puts val into FlatFile under key and returns an error if it occurs.
// Duplicate key produces error.
func (ff *FlatFile) Put(key, val []byte) error {

	ff.mutex.Lock()
	defer ff.mutex.Unlock()

	if err := ff.put(key, val); err != nil {
		return err
	}
	if ff.mirror != nil {
		if err := ff.mirror.put(key, val); err != nil {
			return fmt.Errorf("mirror error: %w", err)
		}
	}
	return nil
}

// get is the Get implementation.
func (ff *FlatFile) get(key string, walking bool) (blob []byte, err error) {
	cell, ok := ff.header.cells[string(key)]
	if !ok {
		return nil, ErrKeyNotFound
	}
	if cell.CellState == StateDeleted {
		return nil, ErrKeyNotFound
	}
	if cell.Cache != nil {
		blob = cell.Cache
		err = nil
	} else {
		file := ff.stream.pages[cell.PageIndex].file
		if _, err := file.Seek(cell.Offset, os.SEEK_SET); err != nil {
			return nil, fmt.Errorf("stream seek error: %w", err)
		}
		blob = make([]byte, cell.Used)
		if _, err = file.Read(blob); err != nil {
			return nil, fmt.Errorf("stream read error: %w", err)
		}
		if ff.options.CRC && cell.CRC32 != 0 {
			crc := crc32.ChecksumIEEE(blob)
			if crc != cell.CRC32 {
				return nil, ErrChecksumFailed
			}
		}
	}
	if ff.options.MaxCacheMemory > 0 && !walking {
		if cell.Cache == nil {
			cell.key = string(key)
			cell.Cache = blob
		}
		ff.header.cachedCells.Push(cell, ff.options.MaxCacheMemory)
	}
	return
}

// Get gets data from FlatFile with the specified unique id. If an error occurs
// it is returned.
func (ff *FlatFile) Get(key []byte) (blob []byte, err error) {

	ff.mutex.RLock()
	defer ff.mutex.RUnlock()

	return ff.get(string(key), false)
}

// Modify modifies an existing blob specified under key by replacing it with
// specified val. If an error occurs it is returned.
func (ff *FlatFile) Modify(key, val []byte) (err error) {

	if ff.options.Immutable {
		return ErrImmutableFile
	}

	ff.mutex.Lock()
	defer ff.mutex.Unlock()

	if _, ok := ff.header.cells[string(key)]; !ok {
		return ErrKeyNotFound
	}
	if ff.options.MaxPageSize > 0 && int64(len(val)) > ff.options.MaxPageSize {
		return ErrBlobTooBig
	}
	if err = ff.delete(key); err != nil {
		return
	}
	if err := ff.put(key, val); err != nil {
		return err
	}
	if ff.mirror != nil {
		if err := ff.mirror.Modify(key, val); err != nil {
			return fmt.Errorf("mirror error: %w", err)
		}
	}
	return nil
}

// delete is Delete implementation.
func (ff *FlatFile) delete(key []byte) error {

	k := string(key)

	cell, ok := ff.header.cells[k]
	if !ok {
		return ErrKeyNotFound
	}
	if cell.CellState == StateDeleted {
		return nil
	}
	ff.header.cachedCells.Remove(cell)
	ff.header.deletedCells.Push(cell)
	cell.CellState = StateDeleted
	return nil
}

// Delete marks a blob specified under key as deleted. If an error occurs it
// is returned.
func (ff *FlatFile) Delete(key []byte) error {

	if ff.options.Immutable {
		return ErrImmutableFile
	}

	ff.mutex.Lock()
	defer ff.mutex.Unlock()

	if err := ff.delete(key); err != nil {
		return err
	}
	if ff.mirror != nil {
		if err := ff.mirror.Delete(key); err != nil {
			return fmt.Errorf("mirror error: %w", err)
		}
	}
	return nil
}

// Walk walks the FlatFile by calling f with currently enumerated key/value
// pair as parameters. f should return true to continue enumeration.
func (ff *FlatFile) Walk(f func(key, val []byte) bool) error {

	ff.mutex.Lock()
	defer ff.mutex.Unlock()

	for k := range ff.header.cells {
		data, err := ff.get(k, true)
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

// Concat concats header and stream into a temp file then rotates them with
// main files. Writes are locked during Concat.
// Returns an error if one occurs.
func (ff *FlatFile) Concat() error {
	// TODO: Implement concat.

	/*

		hfile, err := os.Open(fmt.Sprintf("%s.%s", ff.header.filename, ConcatExt))
		if err != nil {
			return fmt.Errorf("concat header create failed: %w", err)
		}
		pages
		ff.mutex.Lock()
		defer ff.mutex.Unlock()


		for i := 0; i < ff.stream.len() {

		}

		for ckey, cval := range ff.header.cells {

			// Read.
			if locking {
				ff.mutex.RLock()
			}

			// EndRead
			if locking {
				ff.mutex.RUnlock()
			}

			// Write.
			if locking {
				ff.mutex.Lock()
			}

			// ENdWrite
			if locking {
				ff.mutex.Unlock()
			}

		}
	*/
	return nil
}

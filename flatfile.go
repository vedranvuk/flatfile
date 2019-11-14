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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	HeaderExt  = "header"
	StreamExt  = "stream"
	OptionsExt = "options"
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
)

// Options defines FlatFile options.
type Options struct {

	// filename holds the options filename once options have been persisted.
	filename string

	// TODO MirrorDir specifies a directory where an up-to-date exact mirror
	// copy of the current flatfile will be maintained. If unspecified, no copy
	// is maintained.
	// Default value: [none]
	MirrorDir string

	// TODO CRC specifies if a cell CRC should be done.
	// Available options: crc32.
	// Default value: md5
	CRC bool

	// CachedWrites specifies if write operations should be cached as well.
	// Used only if a cache is defined.
	// Default value: false
	CachedWrites bool

	// MaxCacheMemory specifies maximum cell cache memory to use.
	// If <= 0 it is disabled.
	// Default value: 33554432 (32MB)
	MaxCacheMemory int64

	// MaxPageSize defines maximum size of a stream page. If <= 0, page size is
	// of unlimited size.
	// Default value: 4294967295 (4GB).
	MaxPageSize int64

	// PreallocatePages specifies if new pages should be preallocated when
	// created. This increases page creation time but helps minimize OS disk
	// fragmentation during writes.
	// Default value: true
	PreallocatePages bool

	// PersistentHeader specifies if header file should be immediately appended
	// to disk or kept in memory until FlatFile is closed.
	// Default value: true
	PersistentHeader bool

	// SyncWrites specifies if files should be written synchronously. This
	// circumvents OS write caching, slows down writes considerably and tortures
	// the disk drive. This option applies to header and stream.
	// Default value: false
	SyncWrites bool

	// Immutable specifies if the file is immutable. If true, Modify and Delete
	// will fail.
	// Default value: true
	Immutable bool

	// ZeroPadDeleted specifies if deleted cells should be 0 padded.
	// Default value: true
	ZeroPadDeleted bool
}

// NewOptions returns a new *Options instance.
func NewOptions() *Options {
	p := &Options{}
	p.init()
	return p
}

// init initializes options to default values.
func (o *Options) init() {
	o.MaxPageSize = 4294967295 // 4GB
	o.CachedWrites = false
	o.MaxCacheMemory = 33554432
	o.PreallocatePages = true
	o.PersistentHeader = true
	o.SyncWrites = false
	o.Immutable = true
	o.ZeroPadDeleted = true
}

// Marshal marshals Options to writer w.
func (o *Options) Marshal(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "\t")
	return enc.Encode(o)
}

// Unmarshal unmarshals Options from reader r.
func (o *Options) Unmarshal(r io.Reader) error {
	return json.NewDecoder(r).Decode(o)
}

// FlatFile represents the actual flat file.
type FlatFile struct {
	options *Options
	header  *header
	stream  *stream
	mutex   sync.RWMutex
}

// Open opens an existing or creates a new flatfile. filename is a name of a
// directory where header and stream files consisting flatfile are located.
// Close() should be called after use to free the file descriptors.
func Open(filename string, options *Options) (*FlatFile, error) {

	// TODO Implement file checking on open.

	ff := &FlatFile{
		mutex:   sync.RWMutex{},
		options: options,
		header:  newHeader(),
		stream:  &stream{},
	}
	if ff.options == nil {
		ff.options = NewOptions()
	}

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

	// Load options
	ff.options.filename = filepath.Join(filename, bn+"."+OptionsExt)
	if err := ff.loadOptions(); err != nil {
		return nil, fmt.Errorf("options load error: %w", err)
	}

	// Set header and stream filenames.
	ff.header.filename = filepath.Join(filename, bn+"."+HeaderExt)
	ff.stream.filename = filepath.Join(filename, bn)

	// Open and load the header.
	if err = ff.header.openOrCreate(ff.options.SyncWrites); err != nil {
		return nil, fmt.Errorf("error opening header: %w", err)
	}
	if err = ff.header.load(); err != nil {
		return nil, fmt.Errorf("error loading header: %w", err)
	}

	// Open stream page files.
	if ff.Len() > 0 {
		if err = ff.stream.open(
			ff.header.lastCell().PageIndex+1, ff.options.SyncWrites); err != nil {

			ff.header.close()
			return nil, fmt.Errorf("error opening stream: %w", err)
		}
	}

	return ff, nil
}

// loadOptions loads options.
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
	errh := ff.header.close()
	errs := ff.stream.close()
	if erro != nil || errh != nil || errs != nil {
		return fmt.Errorf(
			"error closing flatfile: options: %s, header %s, stream %s",
			erro, errh, errs)
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
	if c, ok := ff.header.cells[string(key)]; ok {
		if c.CellState != StateDeleted {
			return ErrDuplicateKey
		}
	}
	// Check if data is bigger than page size.
	putsize := len(val)
	if ff.options.MaxPageSize > 0 && int64(putsize) > ff.options.MaxPageSize {
		return ErrBlobTooBig
	}
	// Initialize a cell.
	putcell := ff.header.freeCell(!ff.options.Immutable, int64(putsize))
	if putcell.CellState == StateDeleted {
		putcell.Used = int64(putsize)
		putcell.CellState = StateReused
	}
	// Cache cell if requested.
	if ff.options.CachedWrites && ff.options.MaxCacheMemory > 0 {
		if putcell.Cache == nil {
			putcell.key = string(key)
			putcell.Cache = val
		}
		ff.header.cachedCells.Push(putcell, ff.options.MaxCacheMemory)
	}
	// Select page.
	var page *page
	if putcell.CellState == StateNormal {
		var idx int
		var err error
		// Get current page...
		idx, page, err = ff.stream.currentPage(ff.options.MaxPageSize)
		if err != nil {
			return err
		}
		// ...and advance if required.
		if ff.options.MaxPageSize > 0 {
			if putcell.Offset+putcell.Allocated >= ff.options.MaxPageSize {
				if ff.options.PreallocatePages {
					idx, page, err = ff.stream.newPage(ff.options.MaxPageSize)
				} else {
					idx, page, err = ff.stream.newPage(0)
				}
				if err != nil {
					return err
				}
				putcell.Offset = 0
			}
		}
		putcell.PageIndex = int64(idx)
		ff.header.lastKey = string(key)
	} else {
		// Select page from a reused cell.
		page = ff.stream.pages[putcell.PageIndex]
	}
	// Generate blob checksum.
	if ff.options.CRC {
		// TODO Implement crc32
	}
	// Write blob.
	if _, err := page.file.Seek(int64(putcell.Offset), os.SEEK_SET); err != nil {
		return fmt.Errorf("stream seek error: %w", err)
	}
	if _, err := page.file.Write(val); err != nil {
		return fmt.Errorf("stream write error: %w", err)
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
		ff.header.dirtyCells = append(ff.header.dirtyCells, string(key))
	}
	// Fill the rest with 0s, if requested.
	if !ff.options.Immutable && ff.options.ZeroPadDeleted && putcell.CellState != StateNormal {
		buf := make([]byte, putcell.Allocated-putcell.Used)
		if _, err := page.file.Write(buf); err != nil {
			return fmt.Errorf("stream write error: %w", err)
		}
	}
	// Sync if requested.
	if ff.options.SyncWrites {
		if err := page.file.Sync(); err != nil {
			return fmt.Errorf("stream sync failed: %w", err)
		}
	}
	// Append the cell.
	ff.header.cells[string(key)] = putcell
	return nil
}

// Put puts val into FlatFile under key and returns an error if it occurs.
// Duplicate key produces error.
func (ff *FlatFile) Put(key, val []byte) error {

	ff.mutex.Lock()
	defer ff.mutex.Unlock()

	return ff.put(key, val)
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
		// TODO Verify crc32
		if ff.options.CRC {

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
	return ff.put(key, val)
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

	return ff.delete(key)
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

func (ff *FlatFile) Concat() error {
	// TODO: Implement concat.
	return nil
}

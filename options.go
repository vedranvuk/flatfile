// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package flatfile

import (
	"io"

	"github.com/vedranvuk/binaryex"
)

// Options defines FlatFile options.
type Options struct {

	// MirrorDir specifies a directory where an exact, up-to-date mirror
	// copy of the current flatfile will be maintained. If unspecified,
	// no copy is maintained.
	// Default value: [none]
	MirrorDir string

	// CRC specifies if a cell CRC should be done calculated on Put
	// and checked on Get.
	// Default value: true
	CRC bool

	// MaxCacheMemory specifies maximum cell cache memory to use.
	// If <= 0 it is disabled.
	// Default value: 33554432 (32MB)
	MaxCacheMemory int64

	// CachedWrites specifies if write operations should be cached as well.
	// Used only if a cache is defined.
	// Default value: false
	CachedWrites bool

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

	// Immutable specifies if the file is immutable. If true, Modify and Delete
	// will fail.
	// Default value: false
	Immutable bool

	// SyncWrites specifies if files should be written synchronously. This
	// circumvents OS write caching, slows down writes considerably and tortures
	// the disk drive. This option applies to header and stream.
	// Default value: false
	SyncWrites bool

	// ZeroPadDeleted specifies if deleted cells should be 0 padded.
	// Default value: true
	ZeroPadDeleted bool

	// CompactHeader specifies if the header should be compacted each time when
	// loaded from file on session start.
	// Default value: true
	CompactHeader bool

	// UseIntents specifies if intent files should be used. Any destructive
	// op is described in a separate file before the actual operation. If the
	// operation fails it can be rolled back using the intent file.
	// A recovery system which slows things down quite a bit.
	// Default value: false
	UseIntents bool

	// filename holds the options filename once options have been persisted.
	filename string

	// mirror specifies if this FlatFile is a mirror.
	mirrored bool
}

// NewOptions returns a new *Options instance.
func NewOptions() *Options {
	p := &Options{}
	p.init()
	return p
}

// init initializes options to default values.
func (o *Options) init() {
	o.MirrorDir = ""
	o.CRC = true
	o.MaxCacheMemory = 33554432
	o.CachedWrites = false
	o.MaxPageSize = 4294967295 // 4GB
	o.PreallocatePages = true
	o.PersistentHeader = true
	o.Immutable = false
	o.SyncWrites = false
	o.ZeroPadDeleted = true
	o.CompactHeader = true
	o.UseIntents = false
}

// Marshal marshals Options to writer w.
func (o *Options) Marshal(w io.Writer) error {
	return binaryex.Write(w, o)
}

// Unmarshal unmarshals Options from reader r.
func (o *Options) Unmarshal(r io.Reader) error {
	no := NewOptions()
	no.init()
	if err := binaryex.Read(r, no); err != nil {
		return err
	}
	no.filename = o.filename
	no.mirrored = o.mirrored
	*o = *no
	return nil
}

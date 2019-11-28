// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package flatfile

import (
	"errors"
	"io"
	"os"

	"github.com/vedranvuk/binaryex"
)

// header manages cells and their serialization.
type header struct {

	// filename is the full path to header file.
	filename string

	// file is the underlying header file.
	file *os.File

	// open tells if file is open.
	open bool

	// cells holds and manages all cells.
	cells *pot

	// cache holds and manages cached cells.
	cache *mem

	// trash holds and manages deleted cells.
	trash *bin

	// dirty holds cells that are in-memory only.
	dirty map[CellID]*cell

	// lastKey holds the key of last inserted cell.
	lastKey string

	// keys maps a key to a cell.
	keys map[string]*cell
}

// newHeader creates a new header with specified filename.
func newHeader(filename string) (h *header) {
	h = &header{
		filename: filename,
	}
	return h
}

// hdr is the .header signature.
var hdr = []byte{0xF1, 0x47, 0xF1, 0x13}

// Open opens the header file and loads it or creates it if it doesn't exist.
// Returns index of last stream page that needs to be opened or an error.
func (h *header) Open(compactheader, sync bool) (lastpage int64, err error) {
	lastpage = -1
	opt := os.O_CREATE | os.O_RDWR
	if sync {
		opt = opt | os.O_SYNC
	}
	h.file, err = os.OpenFile(h.filename, opt, os.ModePerm)
	if err != nil {
		return
	}
	if _, err = h.file.Write(hdr[0:]); err != nil {
		return
	}
	if _, err = h.file.Seek(0, 0); err != nil {
		return
	}
	h.cells = newPot()
	h.keys = make(map[string]*cell)
	h.dirty = make(map[CellID]*cell)
	h.trash = newBin()
	h.cache = newMem()
	if lastpage, err = h.load(compactheader); err == nil {
		h.open = true
	}
	return
}

// Close saves dirty cells if they exist and definitely closes the header file.
func (h *header) Close() error {
	errf := h.Flush()
	errc := error(nil)
	if h.file != nil {
		errc = h.file.Close()
		h.file = nil
	}
	h.keys = nil
	h.dirty = nil
	h.trash = nil
	h.cache = nil
	h.open = false
	if errf != nil || errc != nil {
		return ErrFlatFile.Errorf("close failed: flush: %v, close: %v", errf, errc)
	}
	return nil
}

// load loads the cells from the header file.
func (h *header) load(compactheader bool) (lastpage int64, err error) {
	// read header.
	buf := make([]byte, 4)
	if _, err := h.file.Read(buf); err != nil {
		return 0, ErrFlatFile.Errorf("header read failed: %w", err)
	}
	for i, v := range buf {
		if hdr[i] != v {
			return 0, ErrFlatFile.Errorf("invalid header")
		}
	}
	// temp vars.
	cbuf := make([]byte, 64)
	ckey := ""
	csize := 0
	// read till EOF.
	for err == nil {
		cell := &cell{}
		// key.
		if err = binaryex.ReadString(h.file, &ckey); err != nil {
			break
		}
		cell.key = ckey
		// size.
		if err = binaryex.ReadNumber(h.file, &csize); err != nil {
			break
		}
		// cell.
		if _, err = io.ReadFull(h.file, cbuf[:csize]); err != nil {
			break
		}
		if err = cell.UnmarshalBinary(cbuf); err != nil {
			break
		}
		// put cell to pot.
		h.cells.Mask(cell)
	}
	// check err
	if !errors.Is(err, io.EOF) {
		return 0, err
	}
	err = nil
	// update deleted cells.
	maxpage := int64(0)
	h.cells.Walk(func(c *cell) bool {
		if c.CellState == StateDeleted {
			h.trash.Trash(c)
		} else {
			h.keys[c.key] = c
			h.lastKey = c.key
		}
		if c.PageIndex > maxpage {
			maxpage = c.PageIndex
		}
		return true
	})
	// rewrite header file.
	if compactheader {
		if err = h.file.Truncate(0); err != nil {
			return 0, err
		}
		if _, err := h.file.Seek(0, os.SEEK_SET); err != nil {
			return 0, err
		}
		if _, err := h.file.Write(hdr[0:]); err != nil {
			return 0, err
		}
		if err := h.save(); err != nil {
			return 0, err
		}
	}
	return maxpage, err
}

// save saves cells to header.
func (h *header) save() (err error) {
	h.cells.Walk(func(c *cell) bool {
		if err = c.write(h.file, c.key); err != nil {
			return false
		}
		return true
	})
	return
}

// Select is the main cell allocation function. It either returns an
// existing, deleted cell whose Allocated satisfies size requirement
// if reuse is specified, or a new empty cell if none such found or
// reuse wasn't specified.
func (h *header) Select(reuse bool, size int64) (c *cell) {

	if reuse {
		c = h.trash.Recycle(size)
		if c.Allocated >= size {
			if c.CellState != StateDeleted {
				panic("BzZzz...")
			}
			c.CellState = StateReused
			c.Used = size
			return
		}
	}

	c = h.cells.New()
	c.Used = size
	c.Allocated = size
	return
}

// Use marks c as used under c.key.
func (h *header) Use(c *cell) {
	h.keys[string(c.key)] = c
	h.lastKey = c.key
}

// Update updates the cell in the header.
func (h *header) Update(c *cell, immediate bool) error {
	if immediate {
		if _, err := h.file.Seek(0, os.SEEK_END); err != nil {
			return ErrFlatFile.Errorf("header seek error: %w", err)
		}
		if err := c.write(h.file, string(c.key)); err != nil {
			return err
		}
	} else {
		h.Endirty(c)
	}
	return nil
}

// Destroy destroys a cell removing it from the bin.
func (h *header) Destroy(c *cell) {
	h.cells.Destroy(c)
}

// Cache caches val of cell c under key, imposing cache size limit
// then returns the updated cell.
func (h *header) Cache(c *cell, val []byte, limit int64) {
	c.cache = val
	h.cache.Push(c, limit)
}

// UnCache removes a cell from cache.
func (h *header) UnCache(c *cell) {
	h.cache.Remove(c)
}

// Trash marks c as deleted.
func (h *header) Trash(c *cell) {
	h.trash.Trash(c)
}

// Restore removes the cell from the bin.
func (h *header) Restore(c *cell) {
	h.trash.Remove(c)
}

// Endirty marks a cell under specified key as dirty.
func (h *header) Endirty(c *cell) {
	h.dirty[c.CellID] = c
}

// Flush saves any dirty cells to header file.
func (h *header) Flush() (err error) {
	if len(h.dirty) == 0 {
		return
	}
	if _, err := h.file.Seek(0, os.SEEK_END); err != nil {
		return ErrFlatFile.Errorf("header seek error: %w", err)
	}
	for _, cval := range h.dirty {
		if err = cval.write(h.file, cval.key); err != nil {
			return ErrFlatFile.Errorf("header write error: %w", err)
		}
	}
	h.dirty = nil
	return
}

// IsKeyUsed checks if a cell under specified key exists.
func (h *header) IsKeyUsed(key []byte) (used bool) {
	_, used = h.keys[string(key)]
	return
}

// Cell returns a cell by key if found and a truth if it exists.
func (h *header) Cell(key []byte) (c *cell, ok bool) {
	c, ok = h.keys[string(key)]
	return
}

// Keys returns all keys in the header.
func (h *header) Keys() (result [][]byte) {
	result = make([][]byte, len(h.keys))
	i := 0
	for key := range h.keys {
		result[i] = make([]byte, len([]byte(key)))
		copy(result[i], key)
		i++
	}
	return
}

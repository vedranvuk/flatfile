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

	// cells contains all loaded cells.
	cells *pot

	// keys maps a key to a cell.
	keys map[string]*cell

	// lastKey holds the key of last inserted cell.
	lastKey string

	// dirtyCells holds cells that haven't been written to header.
	dirtyCells map[CellID]*cell

	// cellBin is a slice of deleted cells sorted by cell.Allocated.
	cellBin *bin

	// cellCache is a fifo queue of cached cells.
	cellCache *cache
}

// newHeader creates a new header with specified filename.
func newHeader(filename string) (h *header) {
	h = &header{
		filename: filename,
	}
	return h
}

// init initializes the header.
func (h *header) init() {
	h.cells = newPot()
	h.keys = make(map[string]*cell)
	h.dirtyCells = make(map[CellID]*cell)
	h.cellBin = newBin()
	h.cellCache = newCache()
}

// hdr is the .header signature.
var hdr = []byte{0xF1, 0x47, 0xF1, 0x13}

// OpenOrCreate opens the header file or creates it if it doesn't exist.
func (h *header) OpenOrCreate(sync bool) (err error) {
	opt := os.O_CREATE | os.O_RDWR
	if sync {
		opt = opt | os.O_SYNC
	}
	h.file, err = os.OpenFile(h.filename, opt, os.ModePerm)
	if err != nil {
		return
	}
	if _, err := h.file.Write(hdr[0:]); err != nil {
		return err
	}
	if _, err := h.file.Seek(0, 0); err != nil {
		return err
	}
	return
}

// LoadCells loads the cells from the header file.
func (h *header) LoadCells(rewriteheader bool) (lastpage int64, err error) {
	// init.
	h.init()
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
			h.cellBin.Trash(c)
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
	if rewriteheader {
		if err = h.file.Truncate(0); err != nil {
			return 0, err
		}
		if _, err := h.file.Seek(0, os.SEEK_SET); err != nil {
			return 0, err
		}
		if _, err := h.file.Write(hdr[0:]); err != nil {
			return 0, err
		}
		if err := h.saveCells(); err != nil {
			return 0, err
		}
	}
	return maxpage, err
}

// saveCells saves cells to header.
func (h *header) saveCells() (err error) {
	h.cells.Walk(func(c *cell) bool {
		if err = c.write(h.file, c.key); err != nil {
			return false
		}
		return true
	})
	return
}

// SaveAndClearDirty saves dirty cells to header file then clears dirtyCells.
func (h *header) SaveAndClearDirty() (err error) {

	if _, err := h.file.Seek(0, os.SEEK_END); err != nil {
		return ErrFlatFile.Errorf("header seek error: %w", err)
	}

	for _, cval := range h.dirtyCells {
		if err = cval.write(h.file, cval.key); err != nil {
			return
		}
	}
	h.dirtyCells = nil
	h.dirtyCells = make(map[CellID]*cell)

	return nil
}

// GetFreeCell returns a cell that satisfies size requirement. If reuse is
// specified, a deleted cell of size bigger and closest to size is returned.
// If no such cell exists or not specified returns a new cell.
func (h *header) GetFreeCell(reuse bool, size int64) (c *cell) {

	if reuse {
		c = h.cellBin.Recycle(size)
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

// Cache caches val of cell c under key, imposing cache size limit
// then returns the updated cell.
func (h *header) CacheCell(c *cell, val []byte, limit int64) {
	c.cache = val
	h.cellCache.Push(c, limit)
}

// UnCacheCell removes a cell from cache.
func (h *header) UnCacheCell(c *cell) {
	h.cellCache.Remove(c)
}

// Add adds a cell to header.
func (h *header) AddCell(c *cell) {
	h.keys[string(c.key)] = c
	h.lastKey = c.key
}

// Dirty marks a cell under specified key as dirty.
func (h *header) MarkCellDirty(c *cell) {
	h.dirtyCells[c.CellID] = c
}

// TrashCell marks c as deleted.
func (h *header) TrashCell(c *cell) {
	h.cellBin.Trash(c)
}

// UntrashCell removes the cell from the bin.
func (h *header) UntrashCell(c *cell) {
	h.cellBin.Remove(c)
}

// IsKeyUsed checks if a cell under specified key exists.
func (h *header) IsKeyUsed(key string) bool {
	c, exists := h.keys[key]
	if !exists {
		return false
	}
	return c.CellState != StateDeleted
}

// Close saves dirty cells if they exist and definitely closes the header file.
func (h *header) Close() (err error) {

	//TODO Improve

	defer func() {
		h.file.Close()
		h.file = nil
	}()

	if len(h.dirtyCells) > 0 {
		if err = h.SaveAndClearDirty(); err != nil {
			return
		}
	}
	h.keys = nil
	h.dirtyCells = nil
	h.cellBin = nil
	h.cellCache = nil
	return
}

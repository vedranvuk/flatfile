// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package flatfile

import (
	"errors"
	"fmt"
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

	// cells mapa header cells by their keys.
	cells map[string]*cell

	// lastKey holds the key of last inserted cell.
	lastKey string

	// dirtyCells holds cells that haven't been written to header.
	dirtyCells map[string]bool

	// deletedCells is a slice of deleted cells sorted by cell.Allocated.
	deletedCells *deletedCells

	// cachedCells is a fifo queue of cached cells.
	cachedCells *cachedCells
}

// newHeader creates a new header with specified filename.
func newHeader(filename string) *header {
	return &header{
		filename:     filename,
		cells:        make(map[string]*cell),
		dirtyCells:   make(map[string]bool),
		deletedCells: newDeletedCells(),
		cachedCells:  newCachedCells(),
	}
}

// OpenOrCreate opens the header file or creates it if it doesn't exist.
func (h *header) OpenOrCreate(sync bool) (err error) {
	opt := os.O_CREATE | os.O_RDWR
	if sync {
		opt = opt | os.O_SYNC
	}
	h.file, err = os.OpenFile(h.filename, opt, os.ModePerm)
	return
}

// LoadCells loads the cells from the header file.
func (h *header) LoadCells() (err error) {

	buffer := make([]byte, 42)
	cellSize := 0
	name := ""
	for err == nil {

		// Cell key.
		if err = binaryex.ReadString(h.file, &name); err != nil {
			break
		}
		// Duplicate cell keys take precedence, old are trimmed as Delete
		// and Modify allocate new cell structure internally and on-disk
		// cell is packed and cannot be resized, but replaced.
		if _, ok := h.cells[name]; ok {
			delete(h.cells, name)
		}
		// Cell size.
		cell := &cell{}
		if err = binaryex.ReadNumber(h.file, &cellSize); err != nil {
			break
		}
		// Cell.
		if _, err = io.ReadFull(h.file, buffer[:cellSize]); err != nil {
			break
		}
		if err = cell.UnmarshalBinary(buffer); err != nil {
			break
		}
		h.cells[name] = cell
		h.lastKey = name
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return
}

// SaveAndClearDirty saves dirty cells to header file then clears dirtyCells.
func (h *header) SaveAndClearDirty() (err error) {

	if _, err := h.file.Seek(0, os.SEEK_END); err != nil {
		return fmt.Errorf("header seek error: %w", err)
	}

	for key := range h.dirtyCells {
		cell := h.cells[key]
		if err = cell.write(h.file, key); err != nil {
			return
		}
	}
	h.dirtyCells = make(map[string]bool)

	return nil
}

// lastAddedCell returns the last added cell in the header,
// or if the header is empty, a new empty cell.
func (h *header) lastAddedCell() *cell {
	if h.lastKey == "" {
		return &cell{}
	}
	return h.cells[h.lastKey]
}

// MakeCell returns a new cell iinitialized at next write position.
// If reuse is specified, a deleted cell of size bigger and closest to size is
// returned.
func (h *header) MakeCell(reuse bool, size int64) (c *cell) {

	if reuse {
		c = h.deletedCells.Pop(size)
		if c.Allocated >= size {
			if c.CellState != StateDeleted {
				panic("BzZzz...")
			}
			c.CellState = StateReused
			c.Used = size
			return
		}
	}

	c = h.lastAddedCell()
	return &cell{
		PageIndex: c.PageIndex,
		Offset:    c.Offset + int64(c.Allocated),
		Used:      size,
		Allocated: size,
		CellState: StateNormal,
	}
}

// Add adds a cell to header.
func (h *header) AddCell(key string, c *cell) {
	h.cells[string(key)] = c
	h.lastKey = key
}

// Cache caches val of cell c under key, imposing cache size limit
// then returns the updated cell.
func (h *header) CacheCell(c *cell, key, val []byte, limit int64) *cell {
	c.key = string(key)
	c.cache = val
	h.cachedCells.Push(c, limit)
	return c
}

// UnCacheCell removes a cell from cache.
func (h *header) UnCacheCell(c *cell) {
	h.cachedCells.Remove(c)
}

// Dirty marks a cell under specified key as dirty.
func (h *header) MarkCellDirty(key string) {
	h.dirtyCells[key] = true
}

// MarkCellDeleted marks c as deleted.
func (h *header) MarkCellDeleted(c *cell) {
	h.deletedCells.Push(c)
}

// IsKeyUsed checks if a cell under specified key exists.
func (h *header) IsKeyUsed(key string) bool {
	c, exists := h.cells[key]
	return exists && c.CellState != StateDeleted
}

// LastCellPageIndex returns index of page from last written cell.
func (h *header) LastCellPageIndex() int64 {
	if h.lastKey == "" {
		return 0
	}
	return h.cells[h.lastKey].PageIndex
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
	return
}

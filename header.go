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

	// cells is a slice of header cells.
	cells map[string]*cell

	// lastKey holds the key of last inserted cell.
	lastKey string

	// dirtyCells holds in memory only cells and have to be persisted on
	// session end.
	dirtyCells []string

	// deletedCells holds a slice of deleted cells sorted by cell size.
	deletedCells *deletedCells

	// cachedCells holds a ceched cells queue.
	cachedCells *cachedCells
}

// newHeader returns a new empty header.
func newHeader() *header {
	return &header{
		cells:        make(map[string]*cell),
		deletedCells: newDeletedCells(),
		cachedCells:  newCachedCells(),
	}
}

// openOrCreate opens the header file or creates it if it doesn't exist.
func (h *header) openOrCreate(sync bool) (err error) {
	opt := os.O_CREATE | os.O_RDWR
	if sync {
		opt = opt | os.O_SYNC
	}
	h.file, err = os.OpenFile(h.filename, opt, os.ModePerm)
	return
}

// load loads the cells from the header file.
func (h *header) load() (err error) {

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

// saveDirty saves unsaved cells to header file.
func (h *header) saveDirty() (err error) {

	if _, err := h.file.Seek(0, os.SEEK_END); err != nil {
		return fmt.Errorf("header seek error: %w", err)
	}

	for _, key := range h.dirtyCells {
		cell := h.cells[key]
		if err = cell.write(h.file, key); err != nil {
			return
		}
	}
	h.dirtyCells = nil

	return nil
}

// lastCell returns the last cell in the header,
// or if the header is empty, a new empty cell.
func (h *header) lastCell() *cell {
	if h.lastKey == "" {
		return &cell{}
	}
	return h.cells[h.lastKey]
}

// freeCell will return cell marked as deleted that is bigger and closest in
// size to the specified size or a new cell if none such are available, or reuse
// is false. Also returns the key of the reused cell or an empty string if new.
func (h *header) freeCell(reuse bool, size int64) (c *cell) {

	if reuse {
		c = h.deletedCells.Pop(size)
		if c.Allocated >= size {
			if c.CellState != StateDeleted {
				panic("BzZzz...")
			}
			return
		}
	}

	lastCell := h.lastCell()
	return &cell{
		PageIndex: lastCell.PageIndex,
		Offset:    lastCell.Offset + int64(lastCell.Allocated),
		Used:      int64(size),
		Allocated: int64(size),
		CellState: StateNormal,
	}
}

// aves dirty cells if they exist and definitely closes the header file.
func (h *header) close() (err error) {

	defer func() {
		h.file.Close()
		h.file = nil
	}()

	if len(h.dirtyCells) > 0 {
		if err = h.saveDirty(); err != nil {
			return
		}
	}
	return
}

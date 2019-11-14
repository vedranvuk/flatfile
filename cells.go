// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package flatfile

import (
	"bufio"
	"bytes"
	"container/list"
	"fmt"
	"io"
	"sort"

	"github.com/vedranvuk/binaryex"
)

// CellState defines cell's state.
type CellState uint8

const (
	StateNormal  CellState = iota // Normal, first-use cell.
	StateDeleted                  // Cell is marked as deleted, awaits reuse.
	StateReused                   // Cell is being reused.
)

// cell is an entry in the header.
// It defines a blob in the stream.
type cell struct {

	// PageIndex is the index of the stream page where a blob
	// that this cell descibes is written.
	PageIndex int64

	// Offset is the offset of blob in a stream page.
	Offset int64

	// Allocated is the initial size of blob as it was first
	// created. Allocated >= Used.
	Allocated int64

	// Used specified how much of Allocated is used. Used <= Allocated.
	Used int64

	// CellState is current cell state.
	CellState

	// CRC32 is a crc32 checksum of blob data.
	CRC32 uint32

	// Cache is the complete blob, in-memory.
	cache []byte

	// key is used when caching cells.
	key string
}

// MarshalBinary marshals the cell to a bite slice.
func (c *cell) MarshalBinary() (data []byte, err error) {
	buf := bytes.NewBuffer(nil)
	err = binaryex.WriteStruct(buf, c)
	return buf.Bytes(), err
}

// UnmarshalBinary unmarshals a cell from a bite slice.
func (c *cell) UnmarshalBinary(data []byte) error {
	return binaryex.ReadStruct(bytes.NewBuffer(data), c)
}

// write writes the cell to writer w under specified key.
func (c *cell) write(w io.Writer, key string) (err error) {
	writer := bufio.NewWriter(w)
	if err := binaryex.WriteString(writer, key); err != nil {
		return fmt.Errorf("cell write error: %w", err)
	}
	buffer, err := c.MarshalBinary()
	if err != nil {
		return fmt.Errorf("cell write error: %w", err)
	}
	if err = binaryex.WriteNumber(writer, len(buffer)); err != nil {
		return fmt.Errorf("cell write error: %w", err)
	}
	if _, err = writer.Write(buffer); err != nil {
		return fmt.Errorf("cell write error: %w", err)
	}
	if err = writer.Flush(); err != nil {
		return fmt.Errorf("cell write error: %w", err)
	}
	return nil
}

// deletedCells holds a sorted slice of deleted cells, ordered by cell.Allocated.
type deletedCells struct {
	cells []*cell
}

// newDeletedCells returns a new deletedCells instance.
func newDeletedCells() *deletedCells {
	return &deletedCells{}
}

// Push inserts a cell to self by sorted by cell.Allocated.
func (dc *deletedCells) Push(c *cell) {

	if len(dc.cells) == 0 {
		dc.cells = append(dc.cells, c)
		return
	}

	i := sort.Search(len(dc.cells), func(i int) bool {
		return dc.cells[i].Allocated >= c.Allocated
	})

	dc.cells = append(dc.cells, nil)
	copy(dc.cells[i+1:], dc.cells[i:])
	dc.cells[i] = c
	return
}

// Pop returns a deleted cell whose .Allocated is sizeAtLeast or
// an empty cell if none such found.
func (dc *deletedCells) Pop(sizeAtLeast int64) (c *cell) {

	i := sort.Search(len(dc.cells), func(i int) bool {
		return dc.cells[i].Allocated >= sizeAtLeast
	})
	if i < len(dc.cells) {
		if dc.cells[i].Allocated >= sizeAtLeast {
			c = dc.cells[i]
			if i < len(dc.cells)-1 {
				copy(dc.cells[i:], dc.cells[i+1:])
			}
			dc.cells[len(dc.cells)-1] = nil
			dc.cells = dc.cells[:len(dc.cells)-1]
			return
		}
	}
	return &cell{}
}

// Len returns length of deletedCells.
func (dc *deletedCells) Len() int { return len(dc.cells) }

// cachedCells holds cached cells in a FIFO queue.
type cachedCells struct {
	cells *list.List
	keys  map[string]*list.Element
	size  int64
}

// newCachedCells returns a new cachedCells instance.
func newCachedCells() *cachedCells {
	p := &cachedCells{
		cells: list.New(),
		keys:  make(map[string]*list.Element),
	}
	return p
}

// Push pushes a cell to cachedCells queue. It does so by removing cells from
// the front of the queue until enough space for a cell is freed then adds the
// cell. If a cell is already cached, moves it to the back of the queue.
func (cc *cachedCells) Push(c *cell, maxalloc int64) error {

	elem, ok := cc.keys[c.key]
	if ok {
		cc.cells.MoveToBack(elem)
		return nil
	}
	for {
		elem = cc.cells.Front()
		if elem == nil {
			break
		}
		delete(cc.keys, elem.Value.(*cell).key)
		elem.Value.(*cell).key = ""
		elem.Value.(*cell).cache = nil
		cc.cells.Remove(elem)
		if cc.size-c.Used <= maxalloc {
			break
		}
	}
	cc.keys[c.key] = cc.cells.PushBack(c)
	cc.size += c.Used
	return nil
}

// Remove removes a cell from the queue.
func (cc *cachedCells) Remove(c *cell) {
	elem, ok := cc.keys[c.key]
	if ok {
		cc.cells.Remove(elem)
		delete(cc.keys, c.key)
		c.key = ""
		c.cache = nil
	}
}

// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package flatfile

import (
	"bufio"
	"bytes"
	"io"

	"github.com/vedranvuk/binaryex"
)

// CellState defines cell's state.
type CellState uint8

const (
	StateNormal  CellState = iota // Normal, first-use cell.
	StateDeleted                  // Cell is marked as deleted, awaits reuse.
	StateReused                   // Cell is being reused.
)

// CellID is the unique cell id.
type CellID uint64

// cell is an entry in the header. It defines a blob in the stream.
type cell struct {

	// CellID is the unique ID of a cell.
	CellID

	// CellState is current cell state.
	CellState

	// PageIndex is the index of the stream page where a blob
	// that this cell descibes is written.
	PageIndex int64

	// Offset is the offset of blob in a stream page.
	Offset int64

	// Allocated is the initial size of blob as it was first
	// created. It is always > 0 and >= Used.
	Allocated int64

	// Used specified how much of Allocated is used. Used <= Allocated.
	Used int64

	// CRC32 is a crc32 checksum of blob data.
	CRC32 uint32

	// key is used internally, is the key of a cell, if not deleted.
	key string

	// Cache is used internally, is the complete blob, in-memory.
	cache []byte
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
	var buffer []byte
	writer := bufio.NewWriter(w)
	err = binaryex.WriteString(writer, key)
	if err == nil {
		buffer, err = c.MarshalBinary()
	}
	if err == nil {
		binaryex.WriteNumber(writer, len(buffer))
	}
	if err == nil {
		_, err = writer.Write(buffer)
	}
	if err == nil {
		err = writer.Flush()
	}
	if err != nil {
		return ErrFlatFile.Errorf("cell write error: %w", err)
	}
	return
}

// BlobEndPos returns cell blob end position in the stream.
func (c *cell) BlobEndPos() int64 {
	return c.Offset + c.Allocated
}

// Cached returns if cell blob is in memory.
func (c *cell) Cached() bool {
	return len(c.cache) > 0
}

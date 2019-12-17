package flatfile

import (
	"bytes"
	"os"
)

// page defines and manages a stream page on disk.
type page struct {

	// filename is the full fillename of the stream page.
	filename string

	// file is the underlying file of page.
	file *os.File
}

// Put puts blob into page, ofset and bound by c.
// If zeropad, a blob smaller than c.Allocated is zeroed.
func (p *page) Put(c *cell, blob []byte, zeropad bool) (err error) {
	buf := bytes.NewBuffer(nil)
	if _, err = buf.Write(blob); err != nil {
		return ErrFlatFile.Errorf("buffer write error: %w", err)
	}
	if zeropad && c.CellState != StateNormal {
		zb := make([]byte, c.Allocated-c.Used)
		if _, err = buf.Write(zb); err != nil {
			return ErrFlatFile.Errorf("buffer write error: %w", err)
		}
	}
	if _, err = p.file.Seek(c.Offset, os.SEEK_SET); err != nil {
		return ErrFlatFile.Errorf("page seek error: %w", err)
	}
	if _, err = p.file.Write(buf.Bytes()); err != nil {
		return ErrFlatFile.Errorf("page write error: %w", err)
	}
	return
}

// Get returns blob defined by c.
func (p *page) Get(c *cell) (buf []byte, err error) {
	if _, err = p.file.Seek(c.Offset, os.SEEK_SET); err != nil {
		return nil, ErrFlatFile.Errorf("page seek error: %w", err)
	}
	buf = make([]byte, c.Used)
	if _, err := p.file.Read(buf); err != nil {
		return nil, ErrFlatFile.Errorf("page write error: %w", err)
	}
	return
}

// Close closes the underlying page file.
func (p *page) Close() (err error) {
	err = p.file.Close()
	p.file = nil
	return
}

// newPage creates a new page.
// If prealloc and preallocSize > 0, page file is preallocated to preallocSize.
// If sync, file is opened for synchronous I/O.
func newPage(filename string, preallocSize int64, prealloc, sync bool) (p *page, err error) {
	flags := os.O_CREATE | os.O_RDWR
	if sync {
		flags |= os.O_SYNC
	}
	file, err := os.OpenFile(filename, flags, os.ModePerm)
	if err != nil {
		return nil, ErrFlatFile.Errorf("create page file error: %w", err)
	}
	p = &page{
		filename,
		file,
	}
	if !prealloc || preallocSize <= 0 {
		return
	}
	if err = file.Truncate(preallocSize); err != nil {
		return nil, ErrFlatFile.Errorf(
			"truncate error: %s; file close error: %w, file remove error: %s",
			err, file.Close(), os.Remove(filename))
	}
	return
}

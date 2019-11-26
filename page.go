package flatfile

import "os"

import "bytes"

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
	return buf, nil
}

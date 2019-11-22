package flatfile

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// ErrReadSeekCloserLimiter is a base LimitedReadSeekCloser Error
var ErrReadSeekCloserLimiter = errors.New("readseekcloselimiter:")

// ReadSeekCloser defines a combined io.ReadSeeker and io.Closer interface.
type ReadSeekCloser interface {
	io.ReadSeeker
	io.Closer
}

// LimitedReadSeekCloserWrapper wraps a LimitedReadSeekCloser because pointer receivers.
type LimitedReadSeekCloserWrapper struct {
	rsl *LimitedReadSeekCloser
}

// Read calls wrapped ReadSeekLimiter's Read.
func (rslw LimitedReadSeekCloserWrapper) Read(b []byte) (n int, err error) {
	return rslw.rsl.read(b)
}

// Seek calls wrapped ReadSeekLimiter's Seek.
func (rslw LimitedReadSeekCloserWrapper) Seek(offset int64, whence int) (ret int64, err error) {
	return rslw.rsl.seek(offset, whence)
}

func (rslw LimitedReadSeekCloserWrapper) Close() error {
	return rslw.rsl.close()
}

// LimitedReadSeekCloser wraps a file and limits it's read and seek span.
type LimitedReadSeekCloser struct {
	f     *os.File
	fpos  int64
	ipos  int64
	limit int64
}

// NewLimitedReadSeekCloser returns an io.ReadSeeker which starts from offset of f
// and is able to read and seek within +size from that position.
func NewLimitedReadSeekCloser(f *os.File, offset, size int64) (ReadSeekCloser, error) {
	if _, err := f.Seek(offset, os.SEEK_SET); err != nil {
		return nil, err
	}
	return LimitedReadSeekCloserWrapper{&LimitedReadSeekCloser{f, offset, 0, size}}, nil
}

// read is the limited read implementation.
func (rsk *LimitedReadSeekCloser) read(b []byte) (n int, err error) {
	readlim := rsk.limit - rsk.ipos
	if readlim <= 0 {
		return 0, io.EOF
	}
	readlen := int64(len(b))
	if readlen > readlim {
		n, err = rsk.f.Read(b[:readlim])
		return n, io.EOF
	} else {
		n, err = rsk.f.Read(b)
	}
	rsk.ipos += int64(n)
	return
}

// seek is the limited seek implementation.
func (rsk *LimitedReadSeekCloser) seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case os.SEEK_SET:
		if offset < 0 || offset > rsk.limit {
			return 0, fmt.Errorf("%w seek out of bounds", ErrReadSeekCloserLimiter)
		}
		rsk.ipos = offset
	case os.SEEK_CUR:
		if rsk.ipos+offset < 0 || rsk.ipos+offset > rsk.limit {
			return 0, fmt.Errorf("%w seek out of bounds", ErrReadSeekCloserLimiter)
		}
		rsk.ipos += offset
	case os.SEEK_END:
		if rsk.limit+offset < 0 || rsk.limit+offset > rsk.limit {
			return 0, fmt.Errorf("%w seek out of bounds", ErrReadSeekCloserLimiter)
		}
		rsk.limit += offset
	default:
		return 0, fmt.Errorf("%w invalid whence", ErrReadSeekCloserLimiter)
	}
	return rsk.f.Seek(offset, whence)
}

// Close
func (rsk *LimitedReadSeekCloser) close() error {
	return rsk.f.Close()
}

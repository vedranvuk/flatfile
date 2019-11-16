package flatfile

import (
	"errors"
	"fmt"
	"io"
	"os"
)

// ErrReadSeekLimiter is a base ReadSeekLimiter Error
var ErrReadSeekLimiter = errors.New("readseeklimiter:")

// ReadSeekLimiterWrapper wraps a ReadSeekLimiter because pointer receivers.
type ReadSeekLimiterWrapper struct {
	rsl *ReadSeekLimiter
}

// Read calls wrapped ReadSeekLimiter's Read.
func (rslw ReadSeekLimiterWrapper) Read(b []byte) (n int, err error) {
	return rslw.rsl.read(b)
}

// Seek calls wrapped ReadSeekLimiter's Seek.
func (rslw ReadSeekLimiterWrapper) Seek(offset int64, whence int) (ret int64, err error) {
	return rslw.rsl.seek(offset, whence)
}

// ReadSeekLimiter wraps a file and limits it's read and seek span.
type ReadSeekLimiter struct {
	f     *os.File
	fpos  int64
	ipos  int64
	limit int64
}

// NewReadSeekLimiter returns an io.ReadSeeker which starts from offset of f
// and is able to read and seek within +size from that position.
func NewReadSeekLimiter(f *os.File, offset, size int64) (io.ReadSeeker, error) {
	if _, err := f.Seek(offset, os.SEEK_SET); err != nil {
		return nil, err
	}
	return ReadSeekLimiterWrapper{&ReadSeekLimiter{f, offset, 0, size}}, nil
}

// read is the limited read implementation.
func (rsl *ReadSeekLimiter) read(b []byte) (n int, err error) {
	readlen := int64(len(b))
	readlim := rsl.limit - rsl.ipos
	if readlim <= 0 {
		return 0, io.EOF
	}

	if readlen > readlim {
		n, err = rsl.f.Read(b[:readlim])
		return n, io.EOF
	} else {
		n, err = rsl.f.Read(b)
	}
	rsl.ipos += int64(n)
	return
}

// seek is the limited seek implementation.
func (rsl *ReadSeekLimiter) seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	case os.SEEK_SET:
		if offset < 0 || offset > rsl.limit {
			return 0, fmt.Errorf("%w seek out of bounds", ErrReadSeekLimiter)
		}
		rsl.ipos = offset
	case os.SEEK_CUR:
		if rsl.ipos+offset < 0 || rsl.ipos+offset > rsl.limit {
			return 0, fmt.Errorf("%w seek out of bounds", ErrReadSeekLimiter)
		}
		rsl.ipos += offset
	case os.SEEK_END:
		if rsl.limit+offset < 0 || rsl.limit+offset > rsl.limit {
			return 0, fmt.Errorf("%w seek out of bounds", ErrReadSeekLimiter)
		}
		rsl.limit += offset
	default:
		return 0, fmt.Errorf("%w invalid whence", ErrReadSeekLimiter)
	}
	return rsl.f.Seek(offset, whence)
}

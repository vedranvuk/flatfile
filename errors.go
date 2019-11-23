package flatfile

import (
	"errors"
	"fmt"
)

// FlatFileError is the base error of flatfile package.
type FlatFileError struct {
	err error
}

// Error implements error.Error().
func (ffe FlatFileError) Error() string {
	return fmt.Sprintf("flatfile: %s", ffe.err.Error())
}

// Unwrap implements error.Unwrap().
func (ffs FlatFileError) Unwrap() error {
	return ffs.err
}

// Errorf returns a new FlatFileError which wraps an error created from
// format string and arguments.
func (ffe FlatFileError) Errorf(format string, args ...interface{}) FlatFileError {
	return FlatFileError{fmt.Errorf(format, args...)}
}

var (
	// ErrFlatFile is the base generic error.
	ErrFlatFile = FlatFileError{}

	// ErrInvalidKey is returned when an invalid key was specified.
	ErrInvalidKey = FlatFileError{errors.New("invalid key")}

	// ErrKeyNotFound is returned when a blob under specified key is not found.
	ErrKeyNotFound = FlatFileError{errors.New("key not found")}

	// ErrDuplicateKey is returned if a key already exists during Put.
	ErrDuplicateKey = FlatFileError{errors.New("duplicate key")}

	// ErrBlobToBig is returned in a Put or Modify operation when data size
	// exceeds Options.MaxPageSize.
	ErrBlobTooBig = FlatFileError{errors.New("blob too big")}

	// ErrImmutableFile is returned when a Modify or Delete method has been
	// called on a file that is opened as immutable.
	ErrImmutableFile = FlatFileError{errors.New("immutable file")}

	// ErrChecksumFailed is returned if a crc failed after a cell Get.
	ErrChecksumFailed = FlatFileError{errors.New("blob checksum failed")}
)

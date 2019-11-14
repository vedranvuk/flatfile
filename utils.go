package flatfile

import (
	"errors"
	"os"
)

// FileExists checks if a file exists on disk.
// Returns truth or an error if one occured.
func FileExists(filename string) (exists bool, err error) {
	_, err = os.Stat(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package flatfile

import (
	"fmt"
	"os"
)

// page defines an info about a stream page on disk.
type page struct {

	// filename is the full fillename of the stream page.
	filename string

	// file is the underlying file of page.
	file *os.File
}

// stream manages a slice of pages.
type stream struct {

	// filename holds a base filename for a stream page.
	filename string

	// pages holds a slice of stream page infos.
	pages []*page
}

// newStream creates a new stream with specified filename.
func newStream(filename string) *stream {
	return &stream{
		filename: filename,
	}
}

// Open opens stream page files up to maxPageID which specifies the maximum
// id of the page to open.
func (s *stream) Open(maxPageID int64, sync bool) error {

	opt := os.O_RDWR
	if sync {
		opt = opt | os.O_SYNC
	}
	for i := int64(0); i < maxPageID; i++ {
		fn := fmt.Sprintf("%s.%.4d.%s", s.filename, len(s.pages), StreamExt)
		file, err := os.OpenFile(fn, opt, os.ModePerm)
		if err != nil {
			return fmt.Errorf("page file (%s) open error: %w", fn, err)
		}
		p := &page{
			filename: fn,
			file:     file,
		}
		s.pages = append(s.pages, p)
	}
	return nil
}

// newPageFile allocates a file for a new page and returns it or an error.
func (s *stream) newPageFile(filename string, preallocsize int64, prealloc bool) (file *os.File, err error) {
	file, err = os.OpenFile(filename, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return
	}
	if !prealloc || preallocsize <= 0 {
		return
	}
	if err = file.Truncate(preallocsize); err != nil {
		err = fmt.Errorf("truncate error: %s; file closed: %w, file removed: %s",
			err, file.Close(), os.Remove(filename))
		return nil, err
	}
	return
}

// newPage creates a new page and preallocates the underlying file to
// specified preallocSize if > 0.
func (s *stream) newPage(preallocSize int64, prealloc bool) (int, *page, error) {

	fn := fmt.Sprintf("%s.%.4d.%s", s.filename, len(s.pages), StreamExt)
	file, err := s.newPageFile(fn, preallocSize, prealloc)
	if err != nil {
		return -1, nil, fmt.Errorf("error creating new page: %w", err)
	}
	p := &page{
		filename: fn,
		file:     file,
	}
	s.pages = append(s.pages, p)

	return len(s.pages) - 1, p, nil
}

// currentPage returns the current page. If there are no pages in the stream a
// new page is created and preallocated according to preallocSize.
func (s *stream) currentPage(preallocSize int64, prealloc bool) (
	index int, p *page, err error) {

	if len(s.pages) <= 0 {
		return s.newPage(preallocSize, prealloc)
	}

	index = len(s.pages) - 1
	return index, s.pages[index], nil
}

// GetCellPage returns a page for cell c. If no page exists it is created. If
// cell's bounds exceed page sizelimit a new page is created and returned.
// Returns the updated cell, page or an error if one occured.
func (s *stream) GetCellPage(c *cell, sizelimit int64, prealloc bool) (*cell, *page, error) {

	// Reused cells have their pages.
	if c.CellState != StateNormal {
		return c, s.pages[c.PageIndex], nil
	}

	// Get current page...
	idx, page, err := s.currentPage(sizelimit, prealloc)
	if err != nil {
		return c, nil, err
	}
	// ...and advance if required.
	if sizelimit > 0 {
		if c.Offset+c.Allocated >= sizelimit {
			idx, page, err = s.newPage(sizelimit, prealloc)
			if err != nil {
				return c, nil, err
			}
			c.Offset = 0
		}
	}
	c.PageIndex = int64(idx)
	return c, page, nil
}

// Len returns page count.
func (s *stream) Len() int {
	return len(s.pages)
}

// Close closes the stream pages.
func (s *stream) Close() (err error) {
	txt := ""
	for _, v := range s.pages {
		err = v.file.Close()
		if err != nil {
			if txt != "" {
				txt += ", "
			}
			txt += err.Error()
		}
	}
	s.pages = nil
	if txt != "" {
		return fmt.Errorf("page close error: %s", txt)
	}
	return
}

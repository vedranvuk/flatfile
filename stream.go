// Copyright 2019 Vedran Vuk. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package flatfile

import (
	"fmt"
	"os"
)

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
			return ErrFlatFile.Errorf("page file (%s) open error: %w", fn, err)
		}
		p := &page{
			filename: fn,
			file:     file,
		}
		s.pages = append(s.pages, p)
	}
	return nil
}

// addNewPage creates a new page and preallocates the underlying file to
// specified preallocSize if prealloc and preallocSize > 0.
func (s *stream) addNewPage(preallocSize int64, prealloc, sync bool) (idx int, p *page, err error) {

	fn := fmt.Sprintf("%s.%.4d.%s", s.filename, len(s.pages), StreamExt)
	p, err = newPage(fn, preallocSize, prealloc, sync)
	if err != nil {
		return -1, nil, ErrFlatFile.Errorf("error creating new page: %w", err)
	}
	s.pages = append(s.pages, p)
	idx = len(s.pages) - 1
	return
}

// GetCellPage returns a page for cell c. If no page exists it is created. If
// adding the cell to page would exceed pageSizeLimit a new page is created.
// If prealloc, the page is preallocated to pageSizeLimit when created.
//
// GetCellPage modifies c.
//
// Returns the page or an error if one occured.
func (s *stream) GetCellPage(c *cell, pageSizeLimit int64, prealloc, sync bool) (page *page, err error) {
	// Reused cells have existing pages,
	// return cell page by index.
	if c.CellState != StateNormal {
		return s.pages[c.PageIndex], nil
	}
	// Select last page, create if none.
	pageidx := len(s.pages) - 1
	if pageidx < 0 {
		pageidx, page, err = s.addNewPage(pageSizeLimit, prealloc, sync)
		if err != nil {
			return
		}
	} else {
		page = s.pages[pageidx]
	}
	// Create new page if cell overflows current page.
	if pageSizeLimit > 0 {
		if c.Offset+c.Allocated >= pageSizeLimit {
			pageidx, page, err = s.addNewPage(pageSizeLimit, prealloc, sync)
			if err != nil {
				return
			}
			// Update c.
			c.Offset = 0
		}
	}
	// Update c.
	c.PageIndex = int64(pageidx)
	return
}

// Close closes the stream pages.
func (s *stream) Close() error {
	var e error
	for _, pagev := range s.pages {
		if err := pagev.Close(); err != nil {
			err = fmt.Errorf("page '%s' close error: %w", pagev.filename, err)
			if e != nil {
				e = fmt.Errorf(", %w", e)
			} else {
				e = fmt.Errorf("%w", e)
			}
		}
	}
	s.pages = nil
	if e != nil {
		return ErrFlatFile.Errorf("error closing one or more pages: %w", e)
	}
	return nil
}

// Page retrieves a page by index.
func (s *stream) Page(c *cell) *page {
	return s.pages[int(c.PageIndex)]
}

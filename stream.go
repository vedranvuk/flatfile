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

	// file is the page underlying file on disk.s
	file *os.File
}

// stream holds a collection of pages.
type stream struct {

	// filename holds a base filename for a stream page.
	filename string

	// pages holds a slice of stream page infos.
	pages []*page
}

// open opens stream page files. maxPageID specifies the maximum id
// of the page to open.
func (s *stream) open(maxPageID int64, sync bool) error {

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

// newPage creates a new page and preallocates the underlying file to
// specified preallocSize if its value is more than 0.
func (s *stream) newPage(preallocSize int64) (int, *page, error) {

	fn := fmt.Sprintf("%s.%.4d.%s", s.filename, len(s.pages), StreamExt)
	file, err := os.OpenFile(fn, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return -1, nil, fmt.Errorf("error creating new page: %w", err)
	}
	if preallocSize > 0 {
		if err = file.Truncate(preallocSize); err != nil {
			// TODO Handle these.
			file.Close()
			os.Remove(fn)
			return -1, nil, fmt.Errorf("error preallocating new page: %w", err)
		}
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
func (s *stream) currentPage(preallocSize int64) (
	index int, p *page, err error) {

	if len(s.pages) <= 0 {
		return s.newPage(preallocSize)
	}

	index = len(s.pages) - 1
	return index, s.pages[index], nil
}

// close closes the stream pages.
func (s *stream) close() (err error) {
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
	if txt != "" {
		return fmt.Errorf("page close error: %s", txt)
	}
	return
}

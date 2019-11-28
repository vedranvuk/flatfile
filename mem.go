package flatfile

import (
	"container/list"
)

// mem is cell cache as a fifo queue.
// mem modifies cells it holds.
type mem struct {
	cells *list.List
	keys  map[string]*list.Element
	size  int64
}

// Push pushes a cell to cache by removing cells from the front
// until c + cache size fits within maxalloc then adding c to back.
// If c is already cached, moves it to the back.
//
// Push clears the actual c cache when removing from queue.
func (cc *mem) Push(c *cell, maxalloc int64) {

	elem, ok := cc.keys[c.key]
	if ok {
		cc.cells.MoveToBack(elem)
		return
	}
	for {
		elem = cc.cells.Front()
		if elem == nil {
			break
		}
		if cc.size-c.Used < maxalloc {
			break
		}
		cell := cc.cells.Remove(elem).(*cell)
		delete(cc.keys, cell.key)
		cc.size -= cell.Used
		cell.cache = nil
	}
	cc.keys[c.key] = cc.cells.PushBack(c)
	cc.size += c.Used
	return
}

// Remove removes a cell from the cache.
//
// Remove clears the actual c cache when removing from queue.
func (cc *mem) Remove(c *cell) {
	elem, ok := cc.keys[c.key]
	if ok {
		cc.size -= elem.Value.(*cell).Used
		cc.cells.Remove(elem)
		c.cache = nil
		delete(cc.keys, c.key)
	}
}

// newMem returns a new memory cache.
func newMem() *mem {
	p := &mem{
		cells: list.New(),
		keys:  make(map[string]*list.Element),
	}
	return p
}

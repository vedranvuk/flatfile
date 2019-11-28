package flatfile

import (
	"sort"
	"sync"
)

// bin is a slice of deleted cells.
// always ordered by cell.Allocated.
type bin struct {
	mutex   sync.Mutex
	cells   []*cell
	cellids map[CellID]*cell
}

// newBin returns a new bin.
func newBin() *bin {
	return &bin{
		cellids: make(map[CellID]*cell),
	}
}

// Trash inserts c to bin.
func (b *bin) Trash(c *cell) {

	if len(b.cells) == 0 {
		b.cells = append(b.cells, c)
		b.cellids[c.CellID] = c
		return
	}

	i := sort.Search(len(b.cells), func(i int) bool {
		return b.cells[i].Allocated >= c.Allocated
	})
	b.cells = append(b.cells, nil)
	copy(b.cells[i+1:], b.cells[i:])
	b.cells[i] = c
	b.cellids[c.CellID] = c
	return
}

// Recycle returns c whose .Allocated satisfied minsize
// or an empty cell if none such found.
func (b *bin) Recycle(minsize int64) (c *cell) {

	i := sort.Search(len(b.cells), func(i int) bool {
		return b.cells[i].Allocated >= minsize
	})
	if i >= len(b.cells) {
		return &cell{}
	}
	c = b.cells[i]
	b.cells[i] = nil
	delete(b.cellids, c.CellID)
	b.cells = append(b.cells[:i], b.cells[i+1:]...)
	return
}

// Remove removes a cell from the bin.
func (b *bin) Remove(c *cell) bool {

	if _, ok := b.cellids[c.CellID]; !ok {
		return false
	}

	i := sort.Search(len(b.cells), func(i int) bool {
		return b.cells[i].Allocated >= c.Allocated
	})

	if i >= len(b.cells) {
		return false
	}
	if b.cells[i].CellID != c.CellID {
		return false
	}
	delete(b.cellids, c.CellID)
	b.cells[i] = nil
	b.cells = append(b.cells[:i], b.cells[i+1:]...)
	return true
}

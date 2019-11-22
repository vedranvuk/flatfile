package flatfile

import "sort"

// bin is a slice of deleted cells.
// always ordered by cell.Allocated.
type bin struct {
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
		return
	}

	i := sort.Search(len(b.cells), func(i int) bool {
		return b.cells[i].Allocated >= c.Allocated
	})
	if b.cells[i].CellID == c.CellID {
		panic("BzZzz...")
	}
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
	if i < len(b.cells) {
		if b.cells[i].Allocated >= minsize {
			c = b.cells[i]
			delete(b.cellids, c.CellID)
			if i < len(b.cells)-1 {
				copy(b.cells[i:], b.cells[i+1:])
			}
			b.cells[len(b.cells)-1] = nil
			b.cells = b.cells[:len(b.cells)-1]
			return
		}
	}
	return &cell{}
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
	delete(b.cellids, b.cells[i].CellID)
	b.cells[i] = nil
	b.cells = append(b.cells[:i], b.cells[i+1:]...)
	return true
}

// Len returns number of cells in the bin.
func (dc *bin) Len() int { return len(dc.cells) }

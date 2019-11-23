package flatfile

// pot contains all cells in a flatfile.
type pot struct {
	maxid CellID
	cells map[CellID]*cell
}

// newPot returns a new pot.
func newPot() *pot {
	return &pot{
		maxid: 0,
		cells: make(map[CellID]*cell),
	}
}

// New makes a new cell, unique in the flatfile.
// It initializes it at offset after last unique cell.
func (p *pot) New() (c *cell) {
	c = &cell{}
	c.CellState = StateNormal
	if p.maxid > 0 {
		c.Offset = p.cells[p.maxid].BlobEndPos()
	}
	p.maxid++
	p.cells[p.maxid] = c
	c.CellID = p.maxid
	return
}

// Mask puts a cell into pot replacing any cell with the same CellID.
func (p *pot) Mask(c *cell) {
	if c.CellID > p.maxid {
		p.maxid = c.CellID
	}
	p.cells[c.CellID] = c
}

// Walk walks the cells in the pot by calling f. Should f return false, Walk stops.
func (p *pot) Walk(f func(c *cell) bool) {
	for _, cell := range p.cells {
		if !f(cell) {
			break
		}
	}
}

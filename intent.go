package flatfile

// Op defines a FlatFile operation.
type Op int

const (
	OpNone   Op = iota // No-op, undefined.
	OpPut              // Put operation
	OpDelete           // Delete operation
)

type intent struct {

	// Operation specifies which operation this intent represents.
	Operation Op

	// Key is the key under which cell was stored
	// at the time of backup, can be empty.
	Key []byte

	// Cell is the cell backup.
	Cell *cell

	// Blob is the Cell.blob
	Blob []byte
}

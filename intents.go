package flatfile

import (
	"bytes"
	"errors"
	"github.com/vedranvuk/binaryex"
	"io"
	"os"
)

// Op defines a FlatFile operation.
type Op int

const (
	OpNone   Op = iota // No-op, undefined.
	OpPut              // Put operation
	OpDelete           // Delete operation
)

// IntentID is an unique ID of an intent.
type IntentID int32

// intent defines an operation intent.
type intent struct {

	// ID is the intent ID.
	ID IntentID

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

// intents manages intents and the intent file.
type intents struct {

	// filename is the name of the intents file.
	filename string

	// file is the underlying intents file.
	file *os.File

	// ids holds intents mapped by their ids.
	ids map[IntentID]*intent
}

// newIntents creates a new intents file.
func newIntents(filename string) *intents {
	p := &intents{
		filename: filename,
		ids:      make(map[IntentID]*intent),
	}
	return p
}

// load loads the intents file.
func (i *intents) load() (err error) {
	// Open file.
	file, err := os.OpenFile(i.filename, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return ErrFlatFile.Errorf("intents open error: %w", err)
	}
	i.file = file
	// Read intents.
	for {
		reclen := 0
		if err = binaryex.Read(i.file, &reclen); err != nil {
			break
		}
		itt := &intent{}
		if err = binaryex.Read(i.file, itt); err != nil {
			break
		}
		i.ids[itt.ID] = itt
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return ErrFlatFile.Errorf("intents read error: %w", err)
	}
	return nil
}

// writeIntent writes intent to intents file at current pos.
func (i *intents) writeIntent(itt *intent) (err error) {
	buf := bytes.NewBuffer(nil)
	err = binaryex.Write(buf, itt)
	if err == nil {
		err = binaryex.WriteNumber(i.file, buf.Len())
	}
	if err == nil {
		err = binaryex.WriteNumber(i.file, buf.Len())
	}
	if err == nil {
		_, err = i.file.Write(buf.Bytes())
	}
	if err != nil {
		return ErrFlatFile.Errorf("intent write error: %w", err)
	}
	return nil
}

// Close closes the underlying intents file.
func (i *intents) Close() error {
	defer func() { i.file = nil }()
	if i.file != nil {
		return i.file.Close()
	}
	return nil
}

// Promise creates an intent and returns its' id or an error.
func (i *intents) Promise(c *cell, op Op, data []byte) (id IntentID, err error) {
	// TODO Store intent
	return 0, nil
}

// Complete marks an intent under specified id as complete.
func (i *intents) Complete(id IntentID) error {
	// TODO Remove intent
	return nil
}

// Check checks if there are any incomplete intents and returns them.
func (i *intents) Check() (itts []*intent, err error) {
	err = i.load()
	if err != nil {
		return nil, ErrFlatFile.Errorf("intents check error: %w", err)
	}
	for j := 0; j < len(i.ids); j++ {
		itts = append(itts, i.ids[IntentID(j)])
	}
	return
}

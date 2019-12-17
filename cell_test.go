package flatfile

import (
	"reflect"
	"testing"
)

func TestCell(t *testing.T) {

	makecell := func() *cell {
		return &cell{
			CellID:    1337,
			CellState: StateReused,
			PageIndex: 42,
			Offset:    69,
			Allocated: 9001,
			Used:      64,
			CRC32:     80085,
			key:       "mykey",
			cache:     []byte{0x1, 0x2, 0x3, 0x4, 0x5},
		}
	}

	c := makecell()
	bin, err := c.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	if err := c.UnmarshalBinary(bin); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(c, makecell()) {
		t.Fatalf("cell marshaling failed, want:\n%#v\ngot:\n%#v\n", c, makecell())
	}
}

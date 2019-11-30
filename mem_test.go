package flatfile

import (
	"fmt"
	"testing"
)

func TestMem(t *testing.T) {

	// TODO TestMem

	testdata := []*cell{}
	for i := 0; i < 128; i++ {
		c := &cell{
			CellID: CellID(i),
			key:    fmt.Sprintf("cell%.9d", i),
			cache:  []byte{0x1, 0x2, 0x3, 04, 0x5, 0x6, 0x7, 0x8},
			Used:   int64(8),
		}
		testdata = append(testdata, c)
	}

	m := newMem()

	for _, testv := range testdata {
		m.Push(testv, 8)
	}

}

func BenchmarkMemPush(b *testing.B) {

	b.StopTimer()

	testdata := make([]*cell, b.N)
	for i := 0; i < b.N; i++ {
		testdata[i] = &cell{
			CellID: CellID(i),
			key:    fmt.Sprintf("cell%.9d", i),
			cache:  []byte{0x1, 0x2, 0x3, 04, 0x5, 0x6, 0x7, 0x8},
			Used:   int64(8),
		}
	}

	m := newMem()

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		c := testdata[i]
		m.Push(c, 100000000)
	}
}

func BenchmarkMemRemove(b *testing.B) {

	b.StopTimer()

	m := newMem()

	testdata := make([]*cell, b.N)
	for i := 0; i < b.N; i++ {
		c := &cell{
			CellID: CellID(i),
			key:    fmt.Sprintf("cell%.9d", i),
			cache:  []byte{0x1, 0x2, 0x3, 04, 0x5, 0x6, 0x7, 0x8},
			Used:   int64(8),
		}
		testdata[i] = c
		m.Push(c, 100000000)
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		c := testdata[i]
		m.Remove(c)
	}
}

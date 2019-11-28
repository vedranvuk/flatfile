package flatfile

import (
	_ "net/http/pprof"
	"testing"
)

func TestBin(t *testing.T) {

	testdata := []*cell{
		&cell{
			CellID:    0,
			CellState: StateDeleted,
			Allocated: 1,
		},
		&cell{
			CellID:    1,
			CellState: StateDeleted,
			Allocated: 2,
		},
		&cell{
			CellID:    2,
			CellState: StateDeleted,
			Allocated: 4,
		},
		&cell{
			CellID:    3,
			CellState: StateDeleted,
			Allocated: 8,
		},
		&cell{
			CellID:    4,
			CellState: StateDeleted,
			Allocated: 16,
		},
		&cell{
			CellID:    5,
			CellState: StateDeleted,
			Allocated: 32,
		},
		&cell{
			CellID:    6,
			CellState: StateDeleted,
			Allocated: 64,
		},
		&cell{
			CellID:    7,
			CellState: StateDeleted,
			Allocated: 128,
		},
		&cell{
			CellID:    8,
			CellState: StateDeleted,
			Allocated: 256,
		},
		&cell{
			CellID:    9,
			CellState: StateDeleted,
			Allocated: 512,
		},
	}

	b := newBin()

	for i := 0; i < len(testdata); i++ {
		b.Trash(testdata[i])
	}

	c := &cell{}

	c = b.Recycle(127)
	if c.CellID != 7 {
		t.Fatalf("recycle failed, want cell id %d, got %d", 7, c.CellID)
	}
	if b.Remove(c) {
		t.Fatal("remove failed")
	}
	c = b.Recycle(33)
	if c.CellID != 6 {
		t.Fatalf("recycle failed, want cell id %d, got %d", 6, c.CellID)
	}
	if b.Remove(c) {
		t.Fatal("remove failed")
	}
	c = b.Recycle(4)
	if c.CellID != 2 {
		t.Fatalf("recycle failed, want cell id %d, got %d", 2, c.CellID)
	}
	if b.Remove(c) {
		t.Fatal("remove failed")
	}
	c = b.Recycle(1)
	if c.CellID != 0 {
		t.Fatalf("recycle failed, want cell id %d, got %d", 0, c.CellID)
	}
	if b.Remove(c) {
		t.Fatal("remove failed")
	}
	c = b.Recycle(512)
	if c.CellID != 9 {
		t.Fatalf("recycle failed, want cell id %d, got %d", 9, c.CellID)
	}
	if b.Remove(c) {
		t.Fatal("remove failed")
	}
	c = b.Recycle(2)
	if c.CellID != 1 {
		t.Fatalf("recycle failed, want cell id %d, got %d", 1, c.CellID)
	}
	if b.Remove(c) {
		t.Fatal("remove failed")
	}
	c = b.Recycle(256)
	if c.CellID != 8 {
		t.Fatalf("recycle failed, want cell id %d, got %d", 8, c.CellID)
	}
	if b.Remove(c) {
		t.Fatal("remove failed")
	}
	c = b.Recycle(8)
	if c.CellID != 3 {
		t.Fatalf("recycle failed, want cell id %d, got %d", 3, c.CellID)
	}
	if b.Remove(c) {
		t.Fatal("remove failed")
	}
	c = b.Recycle(31)
	if c.CellID != 5 {
		t.Fatalf("recycle failed, want cell id %d, got %d", 5, c.CellID)
	}
	if b.Remove(c) {
		t.Fatal("remove failed")
	}
	c = b.Recycle(16)
	if c.CellID != 4 {
		t.Fatalf("recycle failed, want cell id %d, got %d", 4, c.CellID)
	}
	if b.Remove(c) {
		t.Fatal("remove failed")
	}

}

func BenchmarkBinTrash(b *testing.B) {

	bin := newBin()

	testdata := []*cell{}
	for i := 0; i < b.N; i++ {
		c := &cell{
			CellID:    CellID(i),
			Allocated: int64(i),
		}
		testdata = append(testdata, c)
	}

	for i := 0; i < b.N; i++ {
		bin.Trash(testdata[i])
	}
}

func BenchmarkBinRecycle(b *testing.B) {

	bin := newBin()

	testdata := []*cell{}
	c := &cell{}
	for i := 0; i < b.N; i++ {
		c = &cell{
			CellID:    CellID(i),
			Allocated: int64(i),
		}
		testdata = append(testdata, c)
		bin.Trash(c)
	}
	for i := 0; i < b.N; i++ {
		bin.Recycle(int64(i))
	}
}

/*

// :6060/debug/pprof
func init() {
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()
}

*/

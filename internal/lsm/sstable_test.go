package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
)

func TestBloomFilterNoFalseNegativesAndFalsePositiveRate(t *testing.T) {
	const n = 20_000
	f := NewBloomFilter(n)
	for i := range n {
		f.Add([]byte(fmt.Sprintf("present-%08d", i)))
	}
	encoded, err := f.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	f, err = UnmarshalBloomFilter(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		if !f.MayContain([]byte(fmt.Sprintf("present-%08d", i))) {
			t.Fatalf("false negative for %d", i)
		}
	}
	rng := rand.New(rand.NewPCG(17, 29))
	falsePositives := 0
	for range n {
		if f.MayContain([]byte(fmt.Sprintf("absent-%016x", rng.Uint64()))) {
			falsePositives++
		}
	}
	if rate := float64(falsePositives) / n; rate > .016 {
		t.Fatalf("false-positive rate %0.4f, want <= 0.016", rate)
	}
}

func TestSSTableRoundTripBoundaryAndCRC(t *testing.T) {
	maxValue := bytes.Repeat([]byte{'x'}, 1<<20)
	entries := []TableEntry{
		{Key: nil, Seq: 9, Value: []byte("empty-key")},
		{Key: []byte("a"), Seq: 10, Value: []byte("new")},
		{Key: []byte("a"), Seq: 8, Value: []byte("old")},
		{Key: []byte("gone"), Seq: 11, Tombstone: true},
		{Key: []byte("z"), Seq: 12, Value: maxValue},
	}
	var out bytes.Buffer
	w := NewSSTableWriter(&out)
	for _, entry := range entries {
		if err := w.Add(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenSSTable(bytes.NewReader(out.Bytes()), int64(out.Len()))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range [][]byte{nil, []byte("a"), []byte("gone"), []byte("z")} {
		got, ok, err := r.Get(key)
		if err != nil || !ok {
			t.Fatalf("Get(%q) = %#v, %v, %v", key, got, ok, err)
		}
		if !bytes.Equal(got.Key, key) {
			t.Fatalf("Get(%q) key = %q", key, got.Key)
		}
	}
	got, ok, err := r.Get([]byte("missing"))
	if err != nil || ok || got.Key != nil {
		t.Fatalf("missing = %#v, %v, %v", got, ok, err)
	}
	corrupt := append([]byte(nil), out.Bytes()...)
	corrupt[0] ^= 1
	r, err = OpenSSTable(bytes.NewReader(corrupt), int64(len(corrupt)))
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = r.Get(nil)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupted data block error = %v, want ErrCorrupt", err)
	}
}

func TestSSTableEmptyAndSingleEntry(t *testing.T) {
	for _, entries := range [][]TableEntry{nil, {{Key: []byte("only"), Seq: 1, Value: []byte("v")}}} {
		var out bytes.Buffer
		w := NewSSTableWriter(&out)
		for _, entry := range entries {
			if err := w.Add(entry); err != nil {
				t.Fatal(err)
			}
		}
		if err := w.Finish(); err != nil {
			t.Fatal(err)
		}
		r, err := OpenSSTable(bytes.NewReader(out.Bytes()), int64(out.Len()))
		if err != nil {
			t.Fatal(err)
		}
		_, ok, err := r.Get([]byte("only"))
		if err != nil || ok != (len(entries) == 1) {
			t.Fatalf("entries=%d Get = %v, %v", len(entries), ok, err)
		}
	}
}

func TestSSTableRejectsUnsortedEntries(t *testing.T) {
	w := NewSSTableWriter(&bytes.Buffer{})
	if err := w.Add(TableEntry{Key: []byte("b"), Seq: 1}); err != nil {
		t.Fatal(err)
	}
	if err := w.Add(TableEntry{Key: []byte("a"), Seq: 1}); err == nil {
		t.Fatal("Add accepted unsorted key")
	}
}

func TestSSTableOversizedRecordGetsItsOwnReadableBlock(t *testing.T) {
	large := bytes.Repeat([]byte{'v'}, dataBlockTarget+1)
	var out bytes.Buffer
	w := NewSSTableWriter(&out)
	for _, entry := range []TableEntry{
		{Key: []byte("after"), Seq: 1, Value: []byte("a")},
		{Key: []byte("large"), Seq: 2, Value: large},
		{Key: []byte("z"), Seq: 3, Value: []byte("z")},
	} {
		if err := w.Add(entry); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Finish(); err != nil {
		t.Fatal(err)
	}
	r, err := OpenSSTable(bytes.NewReader(out.Bytes()), int64(out.Len()))
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := r.Get([]byte("large"))
	if err != nil || !ok || !bytes.Equal(got.Value, large) {
		t.Fatalf("large record = %#v, %v, %v", got, ok, err)
	}
}

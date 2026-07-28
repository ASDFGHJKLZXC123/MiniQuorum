package lsm

import (
	"encoding/binary"
	"fmt"
	"math"
)

const (
	bloomBitsPerKey = 10
	bloomHashCount  = 7
	bloomVersion    = 1
)

// BloomFilter is a fixed-size Bloom filter using seven probes derived from two
// independent 64-bit FNV-1a hashes. It is deliberately small and has no
// dependency on a storage or hashing package.
type BloomFilter struct {
	bits []byte
	m    uint64
}

// NewBloomFilter allocates a filter with 10 bits per expected key. A filter
// for zero keys still has one byte so it remains well-formed on disk. It
// returns nil when the requested size overflows its on-disk or in-memory
// representation.
func NewBloomFilter(expectedKeys int) *BloomFilter {
	bits, bytes, ok := bloomSize(expectedKeys)
	if !ok {
		return nil
	}
	return &BloomFilter{bits: make([]byte, bytes), m: bits}
}

// bloomSize converts before multiplying so a large expected-key count cannot
// wrap to a small filter. The byte length is also checked before conversion to
// int for allocation.
func bloomSize(expectedKeys int) (uint64, int, bool) {
	if expectedKeys < 0 {
		expectedKeys = 0
	}
	keys := uint64(expectedKeys)
	if keys > math.MaxUint64/bloomBitsPerKey {
		return 0, 0, false
	}
	bits := keys * bloomBitsPerKey
	if bits < 8 {
		bits = 8
	}
	bytes, ok := bloomByteLen(bits)
	if !ok {
		return 0, 0, false
	}
	return bits, bytes, true
}

func bloomByteLen(bits uint64) (int, bool) {
	bytes := bits / 8
	if bits%8 != 0 {
		bytes++
	}
	if bytes > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int(bytes), true
}

// Add records key in the filter.
func (f *BloomFilter) Add(key []byte) {
	if f == nil || f.m == 0 {
		return
	}
	h1, h2 := bloomHashPair(key)
	for i := uint64(0); i < bloomHashCount; i++ {
		bit := (h1 + i*h2) % f.m
		f.bits[bit/8] |= 1 << (bit % 8)
	}
}

// MayContain returns false only when key is certainly absent.
func (f *BloomFilter) MayContain(key []byte) bool {
	if f == nil || f.m == 0 {
		return false
	}
	h1, h2 := bloomHashPair(key)
	for i := uint64(0); i < bloomHashCount; i++ {
		bit := (h1 + i*h2) % f.m
		if f.bits[bit/8]&(1<<(bit%8)) == 0 {
			return false
		}
	}
	return true
}

// MarshalBinary returns a stable, self-describing encoding of the filter.
func (f *BloomFilter) MarshalBinary() ([]byte, error) {
	if f == nil || f.m == 0 || uint64(len(f.bits)) != (f.m+7)/8 {
		return nil, fmt.Errorf("lsm: invalid bloom filter")
	}
	out := make([]byte, 10+len(f.bits))
	out[0] = bloomVersion
	out[1] = bloomHashCount
	binary.LittleEndian.PutUint64(out[2:10], f.m)
	copy(out[10:], f.bits)
	return out, nil
}

// UnmarshalBloomFilter decodes a filter written by MarshalBinary.
func UnmarshalBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 11 || data[0] != bloomVersion || data[1] != bloomHashCount {
		return nil, fmt.Errorf("lsm: invalid bloom encoding")
	}
	m := binary.LittleEndian.Uint64(data[2:10])
	byteLen, ok := bloomByteLen(m)
	if m == 0 || !ok || len(data)-10 != byteLen {
		return nil, fmt.Errorf("lsm: invalid bloom bit count")
	}
	bits := append([]byte(nil), data[10:]...)
	return &BloomFilter{bits: bits, m: m}, nil
}

func bloomHashPair(key []byte) (uint64, uint64) {
	// The two domain-separated FNV-1a computations are the two 64-bit hash
	// halves used by Kirsch-Mitzenmacher double hashing. h2 must be non-zero.
	h1 := fnv1a64(key, 0xcbf29ce484222325)
	h2 := fnv1a64(key, 0x84222325cbf29ce4)
	h2 ^= h2 >> 33
	h2 *= 0xff51afd7ed558ccd
	h2 ^= h2 >> 33
	if h2 == 0 {
		h2 = 0x9e3779b97f4a7c15
	}
	return h1, h2
}

func fnv1a64(data []byte, seed uint64) uint64 {
	h := seed
	for _, b := range data {
		h ^= uint64(b)
		h *= 0x100000001b3
	}
	return h
}

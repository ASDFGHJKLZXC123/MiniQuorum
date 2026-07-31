package lsm

import (
	"path/filepath"
	"strings"
	"sync"
)

type countingFileClass int

const (
	countingFileUnknown countingFileClass = iota
	countingFileManifest
	countingFileSSTable
)

func classifyCountingFile(path string) countingFileClass {
	base := filepath.Base(path)
	switch {
	case base == manifestFilename:
		return countingFileManifest
	case strings.HasSuffix(base, sstableFilenameSuffix):
		return countingFileSSTable
	default:
		return countingFileUnknown
	}
}

// CountingFS wraps an injected FS and counts per-class read/write calls and bytes.
// It is additive and does not change underlying filesystem behavior.
type CountingFS struct {
	base  FS
	mu    sync.Mutex
	stats CountingStats
}

// CountingStats carries additive read/write counters partitioned by manifest
// versus SSTable file classes.
type CountingStats struct {
	ManifestReadCalls  uint64
	ManifestReadBytes  uint64
	ManifestWriteCalls uint64
	ManifestWriteBytes uint64
	SSTableReadCalls   uint64
	SSTableReadBytes   uint64
	SSTableWriteCalls  uint64
	SSTableWriteBytes  uint64
}

// NewCountingFS returns a counting benchmark seam wrapper over fs.
func NewCountingFS(fs FS) *CountingFS {
	if fs == nil {
		fs = RealFS{}
	}
	return &CountingFS{base: fs}
}

func (fs *CountingFS) addRead(class countingFileClass, bytes int) {
	if bytes < 0 {
		return
	}
	switch class {
	case countingFileManifest:
		fs.stats.ManifestReadCalls++
		fs.stats.ManifestReadBytes += uint64(bytes)
	case countingFileSSTable:
		fs.stats.SSTableReadCalls++
		fs.stats.SSTableReadBytes += uint64(bytes)
	}
}

func (fs *CountingFS) addWrite(class countingFileClass, bytes int) {
	if bytes < 0 {
		return
	}
	switch class {
	case countingFileManifest:
		fs.stats.ManifestWriteCalls++
		fs.stats.ManifestWriteBytes += uint64(bytes)
	case countingFileSSTable:
		fs.stats.SSTableWriteCalls++
		fs.stats.SSTableWriteBytes += uint64(bytes)
	}
}

// Snapshot returns a copy of current counters.
func (fs *CountingFS) Snapshot() CountingStats {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.stats
}

// Reset clears all counters.
func (fs *CountingFS) Reset() {
	fs.mu.Lock()
	fs.stats = CountingStats{}
	fs.mu.Unlock()
}

func (fs *CountingFS) Create(name string) (File, error) {
	file, err := fs.base.Create(name)
	if err != nil {
		return nil, err
	}
	return &countingFile{underlying: file, path: name, class: classifyCountingFile(name), fs: fs}, nil
}

func (fs *CountingFS) Open(name string) (File, error) {
	file, err := fs.base.Open(name)
	if err != nil {
		return nil, err
	}
	return &countingFile{underlying: file, path: name, class: classifyCountingFile(name), fs: fs}, nil
}

func (fs *CountingFS) OpenAppend(name string) (File, error) {
	file, err := fs.base.OpenAppend(name)
	if err != nil {
		return nil, err
	}
	return &countingFile{underlying: file, path: name, class: classifyCountingFile(name), fs: fs}, nil
}

func (fs *CountingFS) SyncDir(name string) error            { return fs.base.SyncDir(name) }
func (fs *CountingFS) Rename(oldName, newName string) error { return fs.base.Rename(oldName, newName) }
func (fs *CountingFS) Link(oldName, newName string) error   { return fs.base.Link(oldName, newName) }
func (fs *CountingFS) Remove(name string) error             { return fs.base.Remove(name) }
func (fs *CountingFS) List(name string) ([]string, error)   { return fs.base.List(name) }
func (fs *CountingFS) Stat(name string) (FileInfo, error)   { return fs.base.Stat(name) }

type countingFile struct {
	underlying File
	path       string
	class      countingFileClass
	fs         *CountingFS
}

func (file *countingFile) recordRead(bytes int) {
	file.fs.mu.Lock()
	file.fs.addRead(file.class, bytes)
	file.fs.mu.Unlock()
}

func (file *countingFile) recordWrite(bytes int) {
	file.fs.mu.Lock()
	file.fs.addWrite(file.class, bytes)
	file.fs.mu.Unlock()
}

func (file *countingFile) Read(p []byte) (int, error) {
	n, err := file.underlying.Read(p)
	file.recordRead(n)
	return n, err
}

func (file *countingFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := file.underlying.ReadAt(p, off)
	file.recordRead(n)
	return n, err
}

func (file *countingFile) Write(p []byte) (int, error) {
	n, err := file.underlying.Write(p)
	file.recordWrite(n)
	return n, err
}

func (file *countingFile) Sync() error               { return file.underlying.Sync() }
func (file *countingFile) Truncate(size int64) error { return file.underlying.Truncate(size) }
func (file *countingFile) Close() error              { return file.underlying.Close() }

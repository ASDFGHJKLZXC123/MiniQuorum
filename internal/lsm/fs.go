package lsm

import (
	"errors"
	"io"
)

var (
	// ErrNotExist and ErrExist let deterministic core code classify namespace
	// results without importing os. Filesystem adapters wrap these sentinels.
	ErrNotExist = errors.New("lsm fs: file does not exist")
	ErrExist    = errors.New("lsm fs: file already exists")
	ErrFSClosed = errors.New("lsm fs: file is closed")

	// ErrSimulatedCrash is returned by SimFS when a scheduled or explicit
	// crash makes further operations unavailable until Recover.
	ErrSimulatedCrash = errors.New("lsm simfs: crashed")
)

// File is the complete file-handle surface used by the LSM. ReaderAt supports
// SSTable block reads; Truncate removes a classified torn manifest tail.
// Close must return ErrFSClosed when the handle is already closed or was
// invalidated by a crash, allowing ownership retries to retire it safely.
type File interface {
	io.Reader
	io.ReaderAt
	io.Writer
	Sync() error
	Truncate(size int64) error
	Close() error
}

// FileInfo is the small, adapter-neutral stat result the LSM needs.
type FileInfo struct {
	Size  int64
	IsDir bool
}

// FS is the single injected filesystem boundary for every LSM file
// operation. Core LSM code does not import os.
type FS interface {
	Create(name string) (File, error)
	Open(name string) (File, error)
	OpenAppend(name string) (File, error)
	SyncDir(name string) error
	Rename(oldName, newName string) error
	Link(oldName, newName string) error
	Remove(name string) error
	List(name string) ([]string, error)
	Stat(name string) (FileInfo, error)
}

// FSOp names one deterministic filesystem operation in SimFS evidence and
// fault schedules.
type FSOp string

const (
	FSOpCreate     FSOp = "create"
	FSOpOpen       FSOp = "open"
	FSOpOpenAppend FSOp = "open-append"
	FSOpRead       FSOp = "read"
	FSOpWrite      FSOp = "write"
	FSOpFileSync   FSOp = "file-sync"
	FSOpDirSync    FSOp = "dir-sync"
	FSOpTruncate   FSOp = "truncate"
	FSOpRename     FSOp = "rename"
	FSOpLink       FSOp = "link"
	FSOpRemove     FSOp = "remove"
	FSOpList       FSOp = "list"
	FSOpStat       FSOp = "stat"
	FSOpClose      FSOp = "close"
)

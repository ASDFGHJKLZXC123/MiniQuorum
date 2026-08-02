package lsm

import (
	"errors"
	"fmt"
	"os"
	"sort"
)

// RealFS is the sole LSM adapter allowed to call os. Its zero value is ready
// for use.
type RealFS struct{}

var _ FS = RealFS{}

type realFile struct {
	*os.File
}

var _ File = (*realFile)(nil)

func (RealFS) Create(name string) (File, error) {
	file, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if err != nil {
		return nil, normalizeRealFSError(err)
	}
	return &realFile{File: file}, nil
}

func (RealFS) Open(name string) (File, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, normalizeRealFSError(err)
	}
	return &realFile{File: file}, nil
}

func (RealFS) OpenAppend(name string) (File, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_APPEND, 0)
	if err != nil {
		return nil, normalizeRealFSError(err)
	}
	return &realFile{File: file}, nil
}

func (file *realFile) Close() error {
	return normalizeRealFSError(file.File.Close())
}

func (RealFS) SyncDir(name string) error {
	dir, err := os.Open(name)
	if err != nil {
		return normalizeRealFSError(err)
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return err
	}
	return dir.Close()
}

func (RealFS) Rename(oldName, newName string) error {
	return normalizeRealFSError(os.Rename(oldName, newName))
}

func (RealFS) Link(oldName, newName string) error {
	return normalizeRealFSError(os.Link(oldName, newName))
}

func (RealFS) Remove(name string) error {
	return normalizeRealFSError(os.Remove(name))
}

func (RealFS) List(name string) ([]string, error) {
	entries, err := os.ReadDir(name)
	if err != nil {
		return nil, normalizeRealFSError(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names, nil
}

func (RealFS) Stat(name string) (FileInfo, error) {
	info, err := os.Stat(name)
	if err != nil {
		return FileInfo{}, normalizeRealFSError(err)
	}
	return FileInfo{Size: info.Size(), IsDir: info.IsDir()}, nil
}

func normalizeRealFSError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("%w: %v", ErrNotExist, err)
	case errors.Is(err, os.ErrExist):
		return fmt.Errorf("%w: %v", ErrExist, err)
	case errors.Is(err, os.ErrClosed):
		return fmt.Errorf("%w: %v", ErrFSClosed, err)
	default:
		return err
	}
}

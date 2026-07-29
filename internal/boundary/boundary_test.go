// Package boundary enforces the deterministic Raft implementation boundary.
package boundary

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var forbiddenImports = map[string]struct{}{
	"time": {}, "math/rand": {}, "crypto/rand": {}, "net": {}, "os": {},
	"io": {}, "sync": {}, "google.golang.org/grpc": {},
}

var lsmForbiddenImports = map[string]struct{}{
	"time": {}, "math/rand": {}, "crypto/rand": {}, "net": {}, "os": {},
	"google.golang.org/grpc": {},
}

func TestRaftBoundary(t *testing.T) {
	root := filepath.Join("..", "raft")
	if err := checkDirectory(root); err != nil {
		t.Fatal(err)
	}
}

func TestLSMBoundary(t *testing.T) {
	root := filepath.Join("..", "lsm")
	if err := checkLSMDirectory(root); err != nil {
		t.Fatal(err)
	}
}

func TestBoundaryDetectsForbiddenImport(t *testing.T) {
	dir := t.TempDir()
	writeSource(t, dir, "package raft\nimport \"time\"\nvar _ = time.Now\n")
	if err := checkDirectory(dir); err == nil || !strings.Contains(err.Error(), "forbidden import") {
		t.Fatalf("checkDirectory() error = %v, want forbidden import", err)
	}
}

func TestBoundaryDetectsGoStatement(t *testing.T) {
	dir := t.TempDir()
	writeSource(t, dir, "package raft\nfunc f() { go f() }\n")
	if err := checkDirectory(dir); err == nil || !strings.Contains(err.Error(), "go statement") {
		t.Fatalf("checkDirectory() error = %v, want go statement", err)
	}
}

func TestLSMBoundaryDetectsForbiddenCoreImportsAndGoStatements(t *testing.T) {
	tests := []struct {
		name   string
		source string
		detail string
	}{
		{name: "os", source: "package lsm\nimport \"os\"\nvar _ = os.ErrNotExist\n", detail: "forbidden import"},
		{name: "wall clock", source: "package lsm\nimport \"time\"\nvar _ = time.Now\n", detail: "forbidden import"},
		{name: "math rand v2", source: "package lsm\nimport \"math/rand/v2\"\nvar _ = rand.Int\n", detail: "forbidden import"},
		{name: "crypto rand", source: "package lsm\nimport \"crypto/rand\"\nvar _ = rand.Reader\n", detail: "forbidden import"},
		{name: "go statement", source: "package lsm\nfunc f() { go f() }\n", detail: "go statement"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			writeNamedSource(t, dir, "core.go", test.source)
			if err := checkLSMDirectory(dir); err == nil || !strings.Contains(err.Error(), test.detail) {
				t.Fatalf("checkLSMDirectory() error = %v, want %s", err, test.detail)
			}
		})
	}
}

func TestLSMBoundaryAllowsOnlyExplicitRealAdapterToImportOS(t *testing.T) {
	dir := t.TempDir()
	writeNamedSource(t, dir, "core.go", "package lsm\nimport \"sync\"\nvar _ sync.Mutex\n")
	writeNamedSource(t, dir, "fs_real.go", "package lsm\nimport \"os\"\nvar _ = os.ErrNotExist\n")
	if err := checkLSMDirectory(dir); err != nil {
		t.Fatalf("checkLSMDirectory() rejected explicit real adapter: %v", err)
	}
	nested := filepath.Join(dir, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	writeNamedSource(t, nested, "fs_real.go", "package lsm\nimport \"os\"\nvar _ = os.ErrNotExist\n")
	if err := checkLSMDirectory(dir); err == nil || !strings.Contains(err.Error(), "forbidden import os") {
		t.Fatalf("checkLSMDirectory() error = %v, want nested non-designated os adapter rejection", err)
	}
}

func writeSource(t *testing.T, dir, source string) {
	t.Helper()
	writeNamedSource(t, dir, "raft.go", source)
}

func writeNamedSource(t *testing.T, dir, name, source string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
}

func checkDirectory(root string) error {
	files, err := filepath.Glob(filepath.Join(root, "*.go"))
	if err != nil {
		return err
	}
	for _, name := range files {
		if err := checkFile(name, forbiddenImports); err != nil {
			return err
		}
	}
	return nil
}

func checkLSMDirectory(root string) error {
	realAdapter := filepath.Clean(filepath.Join(root, "fs_real.go"))
	return filepath.Walk(root, func(name string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() || filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		forbidden := lsmForbiddenImports
		if filepath.Clean(name) == realAdapter {
			forbidden = withoutImport(lsmForbiddenImports, "os")
		}
		if err := checkFile(name, forbidden); err != nil {
			return err
		}
		return nil
	})
}

func checkFile(name string, forbiddenImports map[string]struct{}) error {
	file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
	if err != nil {
		return err
	}
	for _, spec := range file.Imports {
		path := strings.Trim(spec.Path.Value, "\"")
		if isForbiddenImport(path, forbiddenImports) {
			return &boundaryError{file: name, detail: "forbidden import " + path}
		}
	}
	var goStmt *ast.GoStmt
	ast.Inspect(file, func(node ast.Node) bool {
		if statement, ok := node.(*ast.GoStmt); ok {
			goStmt = statement
			return false
		}
		return true
	})
	if goStmt != nil {
		return &boundaryError{file: name, detail: "go statement"}
	}
	return nil
}

func withoutImport(imports map[string]struct{}, allowed string) map[string]struct{} {
	filtered := make(map[string]struct{}, len(imports)-1)
	for path := range imports {
		if path != allowed {
			filtered[path] = struct{}{}
		}
	}
	return filtered
}

func isForbiddenImport(path string, imports map[string]struct{}) bool {
	for forbidden := range imports {
		if path == forbidden || strings.HasPrefix(path, forbidden+"/") {
			return true
		}
	}
	return false
}

type boundaryError struct {
	file   string
	detail string
}

func (e *boundaryError) Error() string { return e.file + ": " + e.detail }

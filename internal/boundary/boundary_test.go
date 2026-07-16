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

func TestRaftBoundary(t *testing.T) {
	root := filepath.Join("..", "raft")
	if err := checkDirectory(root); err != nil {
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

func writeSource(t *testing.T, dir, source string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "raft.go"), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
}

func checkDirectory(root string) error {
	files, err := filepath.Glob(filepath.Join(root, "*.go"))
	if err != nil {
		return err
	}
	for _, name := range files {
		file, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			return err
		}
		for _, spec := range file.Imports {
			path := strings.Trim(spec.Path.Value, "\"")
			if isForbiddenImport(path) {
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
	}
	return nil
}

func isForbiddenImport(path string) bool {
	for forbidden := range forbiddenImports {
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

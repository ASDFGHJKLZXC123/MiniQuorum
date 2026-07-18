package boundary

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestPhase4SimWorkloadBoundary is an AST gate over production checker and sim
// sources. Seeded local math/rand streams are permitted; package-global random
// draws, goroutines, clocks, networking, synchronization, and OS I/O are not.
func TestPhase4SimWorkloadBoundary(t *testing.T) {
	root := filepath.Join("..", "..")
	files := []string{
		filepath.Join(root, "checker", "history.go"),
		filepath.Join(root, "checker", "model.go"),
		filepath.Join(root, "sim", "event.go"),
		filepath.Join(root, "sim", "ready.go"),
		filepath.Join(root, "sim", "sim.go"),
		filepath.Join(root, "sim", "workload.go"),
	}
	workloadFiles, err := filepath.Glob(filepath.Join(root, "sim", "workload", "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range workloadFiles {
		if !strings.HasSuffix(path, "_test.go") {
			files = append(files, path)
		}
	}
	for _, path := range files {
		if err := checkPhase4DeterministicFile(path); err != nil {
			t.Fatal(err)
		}
	}
}

func checkPhase4DeterministicFile(path string) error {
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		return err
	}

	randAliases := make(map[string]struct{})
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return err
		}
		if phase4ImportForbidden(importPath) {
			return fmt.Errorf("%s: forbidden deterministic-sim import %s", path, importPath)
		}
		if importPath == "math/rand" {
			alias := "rand"
			if spec.Name != nil {
				alias = spec.Name.Name
			}
			if alias == "." || alias == "_" {
				return fmt.Errorf("%s: math/rand must use a named local stream", path)
			}
			randAliases[alias] = struct{}{}
		}
	}

	var violation error
	ast.Inspect(file, func(node ast.Node) bool {
		if violation != nil {
			return false
		}
		switch typed := node.(type) {
		case *ast.GoStmt:
			violation = fmt.Errorf("%s: go statement in deterministic sim path", path)
			return false
		case *ast.CallExpr:
			selector, ok := typed.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			identifier, ok := selector.X.(*ast.Ident)
			if !ok {
				return true
			}
			if _, ok := randAliases[identifier.Name]; !ok {
				return true
			}
			if selector.Sel.Name != "New" && selector.Sel.Name != "NewSource" {
				violation = fmt.Errorf("%s: package-global math/rand call %s", path, selector.Sel.Name)
				return false
			}
		}
		return true
	})
	return violation
}

func phase4ImportForbidden(importPath string) bool {
	for _, forbidden := range []string{
		"time",
		"crypto/rand",
		"net",
		"os",
		"io",
		"sync",
		"google.golang.org/grpc",
	} {
		if importPath == forbidden || strings.HasPrefix(importPath, forbidden+"/") {
			return true
		}
	}
	return false
}

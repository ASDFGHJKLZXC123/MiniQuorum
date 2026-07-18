package boundary

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

var phase4ForbiddenImports = []string{
	"time",
	"crypto/rand",
	"net",
	"os",
	"io",
	"log",
	"sync",
	"google.golang.org/grpc",
}

// TestPhase4SimWorkloadBoundary is an AST gate over every production Go source
// beneath checker and sim. Seeded local math/rand streams are permitted;
// package-global random draws, goroutines, channels/selects, clocks,
// networking, synchronization, and process I/O are not.
func TestPhase4SimWorkloadBoundary(t *testing.T) {
	root := filepath.Join("..", "..")
	files, err := phase4ProductionFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		if err := checkPhase4DeterministicFile(path); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPhase4BoundaryDetectorRejectsForbiddenConstructs(t *testing.T) {
	tests := []struct {
		name   string
		source string
		want   string
	}{
		{
			name:   "goroutine",
			source: "package workload\nfunc f() { go f() }\n",
			want:   "go statement",
		},
		{
			name:   "channel type",
			source: "package workload\nvar ch chan int\n",
			want:   "channel type",
		},
		{
			name:   "channel send",
			source: "package workload\nfunc f(ch any) { ch <- 1 }\n",
			want:   "channel send",
		},
		{
			name:   "channel receive",
			source: "package workload\nfunc f(ch any) { _ = <-ch }\n",
			want:   "channel receive",
		},
		{
			name:   "select",
			source: "package workload\nfunc f() { select {} }\n",
			want:   "select statement",
		},
		{
			name:   "close builtin",
			source: "package workload\nfunc f(ch any) { close(ch) }\n",
			want:   "channel close",
		},
		{
			name:   "stderr builtin",
			source: "package workload\nfunc f() { println(1) }\n",
			want:   "process I/O builtin println",
		},
		{
			name:   "fmt stdout",
			source: "package workload\nimport fmtpkg \"fmt\"\nfunc f() { fmtpkg.Printf(\"x\") }\n",
			want:   "process I/O call fmt.Printf",
		},
		{
			name:   "math rand package global",
			source: "package workload\nimport random \"math/rand\"\nfunc f() { _ = random.Intn(2) }\n",
			want:   "package-global math/rand selector Intn",
		},
		{
			name:   "math rand v2 package global",
			source: "package workload\nimport random \"math/rand/v2\"\nfunc f() { _ = random.IntN(2) }\n",
			want:   "package-global math/rand/v2 selector IntN",
		},
		{
			name:   "math rand function value",
			source: "package workload\nimport random \"math/rand\"\nvar draw = random.Int63\n",
			want:   "package-global math/rand selector Int63",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := checkPhase4DeterministicSource("fixture.go", test.source)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("checkPhase4DeterministicSource() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestPhase4BoundaryDetectorRejectsForbiddenImportCategories(t *testing.T) {
	imports := append([]string(nil), phase4ForbiddenImports...)
	imports = append(imports,
		"io/fs",
		"log/slog",
		"net/http",
		"os/exec",
		"sync/atomic",
		"google.golang.org/grpc/codes",
	)
	sort.Strings(imports)

	for _, importPath := range imports {
		t.Run(strings.ReplaceAll(importPath, "/", "_"), func(t *testing.T) {
			source := "package workload\nimport _ " + strconv.Quote(importPath) + "\n"
			err := checkPhase4DeterministicSource("fixture.go", source)
			if err == nil || !strings.Contains(err.Error(), "forbidden deterministic-sim import "+importPath) {
				t.Fatalf("checkPhase4DeterministicSource() error = %v, want forbidden import %s", err, importPath)
			}
		})
	}
}

func TestPhase4BoundaryDetectorAllowsSeededLocalRandomStreams(t *testing.T) {
	source := `package workload

import (
	"fmt"
	random "math/rand"
	randomv2 "math/rand/v2"
)

func seeded() string {
	legacy := random.New(random.NewSource(1))
	_ = legacy.Intn(8)
	modern := randomv2.New(randomv2.NewPCG(1, 2))
	_ = modern.IntN(8)
	shadowed := legacy
	random := shadowed
	_ = random.Int63()
	return fmt.Sprintf("%d", modern.Uint64())
}
`
	if err := checkPhase4DeterministicSource("allowed.go", source); err != nil {
		t.Fatalf("seeded local RNG was rejected: %v", err)
	}
}

func TestPhase4ProductionFileDiscoveryIncludesOnlyCheckerAndSimProduction(t *testing.T) {
	root := t.TempDir()
	for _, name := range []string{
		"checker/history.go",
		"checker/history_test.go",
		"checker/testdata/forbidden_fixture.go",
		"sim/sim.go",
		"sim/workload/runner.go",
		"sim/workload/runner_test.go",
		"sim/testdata/forbidden_fixture.go",
		"sim/.fixtures/forbidden.go",
		"sim/_fixtures/forbidden.go",
		"internal/server/runtime.go",
	} {
		writePhase4Fixture(t, root, name)
	}

	files, err := phase4ProductionFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	for i := range files {
		relative, err := filepath.Rel(root, files[i])
		if err != nil {
			t.Fatal(err)
		}
		files[i] = filepath.ToSlash(relative)
	}
	want := []string{"checker/history.go", "sim/sim.go", "sim/workload/runner.go"}
	if strings.Join(files, "\n") != strings.Join(want, "\n") {
		t.Fatalf("production files = %q, want %q", files, want)
	}
}

func writePhase4Fixture(t *testing.T, root, name string) {
	t.Helper()
	path := filepath.Join(root, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("package fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func phase4ProductionFiles(root string) ([]string, error) {
	var files []string
	for _, relativeRoot := range []string{"checker", "sim"} {
		productionRoot := filepath.Join(root, relativeRoot)
		before := len(files)
		err := filepath.WalkDir(productionRoot, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() {
				if path != productionRoot && phase4IgnoredDirectory(entry.Name()) {
					return filepath.SkipDir
				}
				return nil
			}
			name := entry.Name()
			if filepath.Ext(name) != ".go" || strings.HasSuffix(name, "_test.go") {
				return nil
			}
			files = append(files, path)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("discover Phase 4 production files under %s: %w", productionRoot, err)
		}
		if len(files) == before {
			return nil, fmt.Errorf("no Phase 4 production Go files found under %s", productionRoot)
		}
	}
	sort.Strings(files)
	return files, nil
}

func phase4IgnoredDirectory(name string) bool {
	return name == "testdata" || name == "vendor" || strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")
}

func checkPhase4DeterministicFile(path string) error {
	return checkPhase4DeterministicSource(path, nil)
}

func checkPhase4DeterministicSource(path string, source any) error {
	fileset := token.NewFileSet()
	file, err := parser.ParseFile(fileset, path, source, 0)
	if err != nil {
		return err
	}

	randAliases := make(map[string]string)
	fmtAliases := make(map[string]struct{})
	for _, spec := range file.Imports {
		importPath, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return err
		}
		if phase4ImportForbidden(importPath) {
			return fmt.Errorf("%s: forbidden deterministic-sim import %s", path, importPath)
		}
		switch importPath {
		case "math/rand", "math/rand/v2":
			alias := "rand"
			if spec.Name != nil {
				alias = spec.Name.Name
			}
			if alias == "." || alias == "_" {
				return fmt.Errorf("%s: %s must use a named local stream", path, importPath)
			}
			randAliases[alias] = importPath
		case "fmt":
			alias := "fmt"
			if spec.Name != nil {
				alias = spec.Name.Name
			}
			if alias == "." || alias == "_" {
				return fmt.Errorf("%s: fmt must use a named import in deterministic sim path", path)
			}
			fmtAliases[alias] = struct{}{}
		}
	}

	var violation error
	setViolation := func(node ast.Node, detail string) {
		position := fileset.Position(node.Pos())
		violation = fmt.Errorf("%s:%d:%d: %s", path, position.Line, position.Column, detail)
	}
	ast.Inspect(file, func(node ast.Node) bool {
		if violation != nil {
			return false
		}
		switch typed := node.(type) {
		case *ast.GoStmt:
			setViolation(typed, "go statement in deterministic sim path")
		case *ast.ChanType:
			setViolation(typed, "channel type in deterministic sim path")
		case *ast.SendStmt:
			setViolation(typed, "channel send in deterministic sim path")
		case *ast.SelectStmt:
			setViolation(typed, "select statement in deterministic sim path")
		case *ast.UnaryExpr:
			if typed.Op == token.ARROW {
				setViolation(typed, "channel receive in deterministic sim path")
			}
		case *ast.CallExpr:
			identifier, ok := typed.Fun.(*ast.Ident)
			if ok && identifier.Obj == nil {
				switch identifier.Name {
				case "close":
					setViolation(typed, "channel close in deterministic sim path")
				case "print", "println":
					setViolation(typed, "process I/O builtin "+identifier.Name+" in deterministic sim path")
				}
			}
		case *ast.SelectorExpr:
			identifier, ok := typed.X.(*ast.Ident)
			if !ok || identifier.Obj != nil {
				return true
			}
			if importPath, ok := randAliases[identifier.Name]; ok && !phase4AllowedRandSelector(importPath, typed.Sel.Name) {
				setViolation(typed, "package-global "+importPath+" selector "+typed.Sel.Name)
				return false
			}
			if _, ok := fmtAliases[identifier.Name]; ok && phase4FmtProcessIO(typed.Sel.Name) {
				setViolation(typed, "process I/O call fmt."+typed.Sel.Name+" in deterministic sim path")
			}
		}
		return violation == nil
	})
	return violation
}

func phase4AllowedRandSelector(importPath, selector string) bool {
	allowed := map[string]map[string]struct{}{
		"math/rand": {
			"New": {}, "NewSource": {}, "NewZipf": {},
			"Rand": {}, "Source": {}, "Source64": {}, "Zipf": {},
		},
		"math/rand/v2": {
			"New": {}, "NewChaCha8": {}, "NewPCG": {}, "NewZipf": {},
			"ChaCha8": {}, "PCG": {}, "Rand": {}, "Source": {}, "Zipf": {},
		},
	}
	_, ok := allowed[importPath][selector]
	return ok
}

func phase4FmtProcessIO(selector string) bool {
	switch selector {
	case "Print", "Printf", "Println", "Scan", "Scanf", "Scanln":
		return true
	default:
		return false
	}
}

func phase4ImportForbidden(importPath string) bool {
	for _, forbidden := range phase4ForbiddenImports {
		if importPath == forbidden || strings.HasPrefix(importPath, forbidden+"/") {
			return true
		}
	}
	return false
}

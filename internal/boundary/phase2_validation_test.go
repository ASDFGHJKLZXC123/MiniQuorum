package boundary

import (
	"sort"
	"strings"
	"testing"
)

func TestBoundaryDetectsEveryForbiddenImportCategory(t *testing.T) {
	imports := make([]string, 0, len(forbiddenImports)+5)
	for path := range forbiddenImports {
		imports = append(imports, path)
	}
	// Subpackages prove the category check cannot be bypassed by importing a
	// narrower I/O, networking, randomness, synchronization, or gRPC package.
	imports = append(imports,
		"io/fs",
		"math/rand/v2",
		"net/http",
		"sync/atomic",
		"google.golang.org/grpc/codes",
	)
	sort.Strings(imports)

	for _, path := range imports {
		t.Run(strings.ReplaceAll(path, "/", "_"), func(t *testing.T) {
			dir := t.TempDir()
			writeSource(t, dir, "package raft\nimport _ \""+path+"\"\n")
			if err := checkDirectory(dir); err == nil || !strings.Contains(err.Error(), "forbidden import "+path) {
				t.Fatalf("checkDirectory() error = %v, want forbidden import %s", err, path)
			}
		})
	}
}

package adbc

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
)

func testManager(t *testing.T, download downloaderFunc) *Manager {
	t.Helper()
	return &Manager{
		CacheDir:   t.TempDir(),
		Resolver:   DefaultResolver(),
		installers: make(map[string]*sync.Mutex),
		download:   download,
	}
}

func TestManager_EnsureDriver_Idempotent_Concurrent(t *testing.T) {
	var downloads int32
	m := testManager(t, func(_ string, destDir string) (string, error) {
		atomic.AddInt32(&downloads, 1)
		if err := os.MkdirAll(destDir, 0o755); err != nil {
			return "", err
		}
		lib := filepath.Join(destDir, "libduckdb.so")
		if err := os.WriteFile(lib, []byte("ok"), 0o644); err != nil {
			return "", err
		}
		return lib, nil
	})

	var wg sync.WaitGroup
	results := make(chan string, 20)
	errCh := make(chan error, 20)

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lib, err := m.EnsureDriver("duckdb", "latest")
			if err != nil {
				errCh <- err
				return
			}
			results <- lib
		}()
	}
	wg.Wait()
	close(results)
	close(errCh)

	for err := range errCh {
		t.Fatalf("EnsureDriver failed: %v", err)
	}

	var first string
	for lib := range results {
		if first == "" {
			first = lib
			continue
		}
		if lib != first {
			t.Fatalf("expected deterministic path, got %q and %q", first, lib)
		}
	}

	if got := atomic.LoadInt32(&downloads); got != 1 {
		t.Fatalf("expected one download, got %d", got)
	}
}

func TestManager_EnsureDriver_RepairsPartialInstall(t *testing.T) {
	m := testManager(t, func(_ string, destDir string) (string, error) {
		lib := filepath.Join(destDir, "libduckdb.so")
		if err := os.MkdirAll(destDir, 0o755); err != nil {
			return "", err
		}
		if err := os.WriteFile(lib, []byte("ok"), 0o644); err != nil {
			return "", err
		}
		return lib, nil
	})

	platform := CurrentPlatform().Tuple()
	partialDir := filepath.Join(m.CacheDir, "duckdb", "latest", platform)
	if err := os.MkdirAll(partialDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(partialDir, "README.txt"), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}

	lib, err := m.EnsureDriver("duckdb", "latest")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(lib) != "libduckdb.so" {
		t.Fatalf("unexpected library path: %s", lib)
	}
}

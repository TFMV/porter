package adbc

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testManager(t *testing.T) *Manager {
	t.Helper()
	return &Manager{
		CacheDir: t.TempDir(),
		Resolver: DefaultResolver(),
	}
}

func TestManager_EnsureRequiredDrivers(t *testing.T) {
	m := testManager(t)
	tuple := CurrentPlatform().Tuple()

	mustWrite := func(path string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("ok"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	mustWrite(filepath.Join(m.CacheDir, "duckdb", "latest", tuple, "libduckdb.so"))
	mustWrite(filepath.Join(m.CacheDir, "flightsql", "latest", tuple, "libflightsql.so"))

	resolved, err := m.EnsureRequiredDrivers()
	if err != nil {
		t.Fatalf("EnsureRequiredDrivers failed: %v", err)
	}
	if _, ok := resolved["duckdb"]; !ok {
		t.Fatal("expected duckdb driver")
	}
	if _, ok := resolved["flightsql"]; !ok {
		t.Fatal("expected flightsql driver")
	}
}

func TestManager_EnsureRequiredDrivers_MissingSingle(t *testing.T) {
	m := testManager(t)
	tuple := CurrentPlatform().Tuple()
	lib := filepath.Join(m.CacheDir, "duckdb", "latest", tuple, "libduckdb.so")
	if err := os.MkdirAll(filepath.Dir(lib), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(lib, []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, err := m.EnsureRequiredDrivers()
	if err == nil {
		t.Fatal("expected missing driver error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "- flightsql") {
		t.Fatalf("expected flightsql in message, got %q", msg)
	}
	if strings.Contains(msg, "dbc install duckdb") {
		t.Fatalf("did not expect duckdb install hint in message, got %q", msg)
	}
	if !strings.Contains(msg, "dbc install flightsql") {
		t.Fatalf("expected flightsql install hint in message, got %q", msg)
	}
}

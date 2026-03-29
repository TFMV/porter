package adbc

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultResolver_BaseURL(t *testing.T) {
	r := DefaultResolver()

	if r.BaseURL == nil {
		t.Fatal("expected base URL")
	}

	if r.BaseURL.Scheme != "https" {
		t.Fatalf("expected https scheme, got %s", r.BaseURL.Scheme)
	}
}

func TestResolver_EnvOverride(t *testing.T) {
	t.Setenv("DBC_DRIVER_BASE_URL", "https://example.com/drivers")
	r := DefaultResolver()

	if r.BaseURL.Host != "example.com" {
		t.Fatalf("expected override host, got %s", r.BaseURL.Host)
	}
}

func TestResolver_ResolvePath(t *testing.T) {
	r := DefaultResolver()

	u, err := r.Resolve(
		Driver{Name: "duckdb"},
		"1.2.3",
		Platform{OS: "linux", Arch: "amd64"},
	)

	if err != nil {
		t.Fatal(err)
	}

	expected := "/drivers/duckdb/1.2.3/duckdb_linux_amd64-1.2.3.tar.gz"
	if u.Path != expected {
		t.Fatalf("expected %s, got %s", expected, u.Path)
	}
}

func TestResolver_DiscoverInstalled_Deterministic(t *testing.T) {
	r := DefaultResolver()
	cache := t.TempDir()
	tuple := Platform{OS: "linux", Arch: "amd64"}.Tuple()

	mustWrite := func(path string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	mustWrite(filepath.Join(cache, "zdriver", "2.0.0", tuple, "libzdriver.so"))
	mustWrite(filepath.Join(cache, "duckdb", "1.0.0", tuple, "libduckdb.so"))
	mustWrite(filepath.Join(cache, "duckdb", "0.9.0", tuple, "libduckdb.so"))

	installed, err := r.DiscoverInstalled(cache, Platform{OS: "linux", Arch: "amd64"})
	if err != nil {
		t.Fatal(err)
	}
	if len(installed) != 3 {
		t.Fatalf("expected 3 installed drivers, got %d", len(installed))
	}

	if installed[0].Name != "duckdb" || installed[0].Version != "0.9.0" {
		t.Fatalf("unexpected first driver: %+v", installed[0])
	}
	if installed[1].Name != "duckdb" || installed[1].Version != "1.0.0" {
		t.Fatalf("unexpected second driver: %+v", installed[1])
	}
	if installed[2].Name != "zdriver" || installed[2].Version != "2.0.0" {
		t.Fatalf("unexpected third driver: %+v", installed[2])
	}
}

package adbc

import (
	"os"
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
	os.Setenv("DBC_DRIVER_BASE_URL", "https://example.com/drivers")
	defer os.Unsetenv("DBC_DRIVER_BASE_URL")

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

	if u.Scheme == "" || u.Host == "" {
		t.Fatalf("invalid url: %s", u.String())
	}

	expected := "/drivers/duckdb/1.2.3/duckdb_linux_amd64-1.2.3.tar.gz"
	if u.Path != expected {
		t.Fatalf("expected %s, got %s", expected, u.Path)
	}
}

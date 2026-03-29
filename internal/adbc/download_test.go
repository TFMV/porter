package adbc

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func createTestTar(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	for name, content := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}

	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gw.Close(); err != nil {
		t.Fatal(err)
	}

	return buf.Bytes()
}

func TestDownloadAndExtract(t *testing.T) {
	tarData := createTestTar(t, map[string]string{"libduckdb.so": "fake-binary"})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(tarData)
	}))
	defer srv.Close()

	dir := t.TempDir()

	path, err := DownloadAndExtract(srv.URL, filepath.Join(dir, "driver"))
	if err != nil {
		t.Fatal(err)
	}

	if filepath.Ext(path) != ".so" {
		t.Fatalf("expected .so file, got %s", path)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("file not found: %v", err)
	}
}

func TestDownloadAndExtract_IdempotentReplacement(t *testing.T) {
	var version int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		version++
		tarData := createTestTar(t, map[string]string{"libduckdb.so": fmt.Sprintf("v%d", version)})
		_, _ = w.Write(tarData)
	}))
	defer srv.Close()

	dest := filepath.Join(t.TempDir(), "driver")
	first, err := DownloadAndExtract(srv.URL, dest)
	if err != nil {
		t.Fatal(err)
	}
	second, err := DownloadAndExtract(srv.URL, dest)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("expected deterministic driver path, got %q and %q", first, second)
	}
}

func TestDownloadAndExtract_RejectsCorruptArchive(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not-a-gzip"))
	}))
	defer srv.Close()

	_, err := DownloadAndExtract(srv.URL, filepath.Join(t.TempDir(), "driver"))
	if err == nil {
		t.Fatal("expected error for corrupt archive")
	}
}

func TestDownloadAndExtract_RejectsPathTraversal(t *testing.T) {
	tarData := createTestTar(t, map[string]string{"../escape.so": "x"})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(tarData)
	}))
	defer srv.Close()

	_, err := DownloadAndExtract(srv.URL, filepath.Join(t.TempDir(), "driver"))
	if err == nil {
		t.Fatal("expected path traversal error")
	}
}

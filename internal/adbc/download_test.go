package adbc

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func createTestTar() []byte {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	files := map[string]string{
		"libduckdb.so": "fake-binary",
	}

	for name, content := range files {
		_ = tw.WriteHeader(&tar.Header{
			Name: name,
			Mode: 0644,
			Size: int64(len(content)),
		})
		_, _ = tw.Write([]byte(content))
	}

	tw.Close()
	gw.Close()

	return buf.Bytes()
}

func TestDownloadAndExtract(t *testing.T) {
	tarData := createTestTar()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write(tarData)
	}))
	defer srv.Close()

	dir := t.TempDir()

	path, err := DownloadAndExtract(srv.URL, dir)
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

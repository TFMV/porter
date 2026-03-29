package adbc

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

func DownloadAndExtract(downloadURL string, destDir string) (string, error) {
	resp, err := http.Get(downloadURL)
	if err != nil {
		return "", fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("bad status: %s", resp.Status)
	}

	parent := filepath.Dir(destDir)
	if err := os.MkdirAll(parent, 0o755); err != nil {
		return "", err
	}
	tmpDir, err := os.MkdirTemp(parent, ".install-*")
	if err != nil {
		return "", fmt.Errorf("create temp dir: %w", err)
	}
	defer os.RemoveAll(tmpDir)

	driverPath, err := extractArchive(resp.Body, tmpDir)
	if err != nil {
		return "", err
	}

	if err := os.RemoveAll(destDir); err != nil && !os.IsNotExist(err) {
		return "", fmt.Errorf("reset destination: %w", err)
	}
	if err := os.Rename(tmpDir, destDir); err != nil {
		return "", fmt.Errorf("activate install: %w", err)
	}

	relPath, err := filepath.Rel(tmpDir, driverPath)
	if err != nil {
		return "", fmt.Errorf("resolve installed driver path: %w", err)
	}
	return filepath.Join(destDir, relPath), nil
}

func extractArchive(body io.Reader, destDir string) (string, error) {
	gzr, err := gzip.NewReader(body)
	if err != nil {
		return "", fmt.Errorf("invalid gzip stream: %w", err)
	}
	defer gzr.Close()

	tr := tar.NewReader(gzr)

	var driverPath string

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("invalid tar stream: %w", err)
		}

		target := filepath.Join(destDir, hdr.Name)
		cleanTarget := filepath.Clean(target)
		cleanDest := filepath.Clean(destDir)
		if cleanTarget != cleanDest && !strings.HasPrefix(cleanTarget, cleanDest+string(os.PathSeparator)) {
			return "", fmt.Errorf("illegal file path in archive: %s", hdr.Name)
		}

		switch hdr.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				return "", err
			}

		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return "", err
			}

			f, err := os.Create(target)
			if err != nil {
				return "", err
			}

			_, copyErr := io.Copy(f, tr)
			closeErr := f.Close()

			if copyErr != nil {
				return "", copyErr
			}
			if closeErr != nil {
				return "", closeErr
			}

			if isDriverLib(target) {
				if driverPath != "" {
					return "", fmt.Errorf("multiple driver libraries found in archive")
				}
				driverPath = target
			}
		}
	}

	if driverPath == "" {
		return "", fmt.Errorf("no driver library found in archive")
	}

	return driverPath, nil
}

func isDriverLib(path string) bool {
	switch filepath.Ext(path) {
	case ".so", ".dylib", ".dll":
		return true
	default:
		return false
	}
}

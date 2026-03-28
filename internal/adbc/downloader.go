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

func DownloadAndExtract(url string, destDir string) (string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return "", fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("bad status: %s", resp.Status)
	}

	if err := os.MkdirAll(destDir, 0o755); err != nil {
		return "", err
	}

	gzr, err := gzip.NewReader(resp.Body)
	if err != nil {
		return "", err
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
			return "", err
		}

		// 🔐 prevent path traversal attacks
		target := filepath.Join(destDir, hdr.Name)
		if !strings.HasPrefix(filepath.Clean(target), filepath.Clean(destDir)) {
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

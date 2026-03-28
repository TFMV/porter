package adbc

import (
	"fmt"
	"os"
	"path/filepath"
)

type Manager struct {
	CacheDir string
	Resolver *Resolver
}

func NewManager() (*Manager, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return nil, err
	}

	return &Manager{
		CacheDir: filepath.Join(dir, "porter", "adbc"),
		Resolver: DefaultResolver(),
	}, nil
}

func (m *Manager) EnsureDriver(name string, version string) (string, error) {
	platform := CurrentPlatform()

	driverDir := filepath.Join(
		m.CacheDir,
		name,
		version,
		platform.Tuple(),
	)

	if lib, err := findDriverLib(driverDir); err == nil {
		return lib, nil
	}

	driver := Driver{Name: name}

	u, err := m.Resolver.Resolve(driver, version, platform)
	if err != nil {
		return "", err
	}

	path, err := DownloadAndExtract(u.String(), driverDir)
	if err != nil {
		return "", err
	}

	return path, nil
}

func findDriverLib(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}

	for _, e := range entries {
		if isDriverLib(e.Name()) {
			return filepath.Join(dir, e.Name()), nil
		}
	}

	return "", fmt.Errorf("driver not found in %s", dir)
}

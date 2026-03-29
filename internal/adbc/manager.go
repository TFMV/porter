package adbc

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

const (
	DefaultDriverName    = "duckdb"
	DefaultDriverVersion = "latest"
)

type downloaderFunc func(downloadURL string, destDir string) (string, error)

type Manager struct {
	CacheDir string
	Resolver *Resolver

	mu         sync.Mutex
	installers map[string]*sync.Mutex
	download   downloaderFunc
}

func NewManager() (*Manager, error) {
	dir, err := os.UserCacheDir()
	if err != nil {
		return nil, err
	}

	return &Manager{
		CacheDir:   filepath.Join(dir, "porter", "adbc"),
		Resolver:   DefaultResolver(),
		installers: make(map[string]*sync.Mutex),
		download:   DownloadAndExtract,
	}, nil
}

func (m *Manager) EnsureDefaultDriver() (InstalledDriver, error) {
	lib, err := m.EnsureDriver(DefaultDriverName, DefaultDriverVersion)
	if err != nil {
		return InstalledDriver{}, err
	}
	return InstalledDriver{Name: DefaultDriverName, Version: DefaultDriverVersion, LibPath: lib}, nil
}

func (m *Manager) DefaultInstalledDriver() (InstalledDriver, error) {
	installed, err := m.Resolver.DiscoverInstalled(m.CacheDir, CurrentPlatform())
	if err != nil {
		return InstalledDriver{}, err
	}
	if len(installed) == 0 {
		return InstalledDriver{}, fmt.Errorf("no installed adbc drivers in %s", m.CacheDir)
	}
	return installed[0], nil
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

	lock := m.installLock(name, version, platform.Tuple())
	lock.Lock()
	defer lock.Unlock()

	// Re-check after acquiring lock to keep installs idempotent and deterministic.
	if lib, err := findDriverLib(driverDir); err == nil {
		return lib, nil
	}

	driver := Driver{Name: name}

	u, err := m.Resolver.Resolve(driver, version, platform)
	if err != nil {
		return "", fmt.Errorf("resolve driver %s@%s: %w", name, version, err)
	}

	path, err := m.download(u.String(), driverDir)
	if err != nil {
		return "", fmt.Errorf("install driver %s@%s: %w", name, version, err)
	}

	return path, nil
}

func (m *Manager) installLock(name, version, tuple string) *sync.Mutex {
	key := name + "|" + version + "|" + tuple
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.installers == nil {
		m.installers = make(map[string]*sync.Mutex)
	}
	lock, ok := m.installers[key]
	if !ok {
		lock = &sync.Mutex{}
		m.installers[key] = lock
	}
	return lock
}

func findDriverLib(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}

	var libs []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if isDriverLib(e.Name()) {
			libs = append(libs, e.Name())
		}
	}

	if len(libs) == 0 {
		return "", fmt.Errorf("driver not found in %s", dir)
	}
	if len(libs) > 1 {
		return "", fmt.Errorf("multiple driver libraries found in %s", dir)
	}
	return filepath.Join(dir, libs[0]), nil
}

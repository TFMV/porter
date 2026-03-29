package adbc

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var requiredDrivers = []string{"duckdb", "flightsql"}

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

func (m *Manager) EnsureRequiredDrivers() (map[string]InstalledDriver, error) {
	installed, err := m.Resolver.DiscoverInstalled(m.CacheDir, CurrentPlatform())
	if err != nil {
		return nil, fmt.Errorf("discover installed adbc drivers: %w", err)
	}

	resolved := make(map[string]InstalledDriver, len(requiredDrivers))
	for _, want := range requiredDrivers {
		for _, drv := range installed {
			if driverNameMatches(drv.Name, want) {
				resolved[want] = drv
				break
			}
		}
	}

	var missing []string
	for _, want := range requiredDrivers {
		if _, ok := resolved[want]; !ok {
			missing = append(missing, want)
		}
	}
	if len(missing) > 0 {
		return nil, MissingDriversError{Missing: missing}
	}

	return resolved, nil
}

type MissingDriversError struct {
	Missing []string
}

func (e MissingDriversError) Error() string {
	missing := append([]string(nil), e.Missing...)
	sort.Strings(missing)

	var b strings.Builder
	b.WriteString("Missing required ADBC drivers.\n\n")
	b.WriteString("Porter requires the following drivers:\n")
	for _, name := range missing {
		b.WriteString("  - ")
		b.WriteString(name)
		b.WriteString("\n")
	}
	b.WriteString("\nInstall them using dbc:\n\n")
	b.WriteString("  curl -LsSf https://dbc.columnar.tech/install.sh | sh\n")
	for _, name := range missing {
		b.WriteString("  dbc install ")
		b.WriteString(name)
		b.WriteString("\n")
	}

	return strings.TrimRight(b.String(), "\n")
}

func driverNameMatches(found string, want string) bool {
	found = strings.ToLower(found)
	want = strings.ToLower(want)

	return found == want || strings.Contains(found, want)
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

package adbc

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type Resolver struct {
	BaseURL *url.URL
}

type InstalledDriver struct {
	Name    string
	Version string
	LibPath string
}

func DefaultResolver() *Resolver {
	base := "https://dbc-cdn.columnar.tech/drivers"

	if v := os.Getenv("DBC_DRIVER_BASE_URL"); v != "" {
		base = v
	}

	u, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil {
		panic(fmt.Sprintf("invalid base url: %v", err))
	}

	return &Resolver{BaseURL: u}
}

func (r *Resolver) Resolve(d Driver, version string, p Platform) (*url.URL, error) {
	tuple := p.Tuple()

	path := fmt.Sprintf(
		"%s/%s/%s_%s-%s.tar.gz",
		d.Name,
		version,
		d.Name,
		tuple,
		version,
	)

	u := *r.BaseURL
	u.Path = strings.TrimRight(u.Path, "/") + "/" + path

	return &u, nil
}

func (r *Resolver) DiscoverInstalled(cacheDir string, p Platform) ([]InstalledDriver, error) {
	var all []InstalledDriver

	dirsToSearch := []string{cacheDir}

	if adbcDriversPath := os.Getenv("ADBC_DRIVER_PATH"); adbcDriversPath != "" {
		for _, dir := range filepath.SplitList(adbcDriversPath) {
			if dir != "" {
				dirsToSearch = append(dirsToSearch, dir)
			}
		}
	}

	adbcDriversDir, err := adbcDriversDir()
	if err == nil {
		dirsToSearch = append(dirsToSearch, adbcDriversDir)
	}

	for _, baseDir := range dirsToSearch {
		drivers, err := r.discoverFromDir(baseDir, p)
		if err != nil {
			continue
		}
		all = append(all, drivers...)
	}

	sort.Slice(all, func(i, j int) bool {
		if all[i].Name != all[j].Name {
			return all[i].Name < all[j].Name
		}
		if all[i].Version != all[j].Version {
			return all[i].Version < all[j].Version
		}
		return all[i].LibPath < all[j].LibPath
	})

	return all, nil
}

func (r *Resolver) discoverFromDir(baseDir string, p Platform) ([]InstalledDriver, error) {
	tuple := p.Tuple()
	var installed []InstalledDriver

	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		driverName := entry.Name()

		driverPath := filepath.Join(baseDir, driverName)
		info, err := os.Stat(driverPath)
		if err != nil {
			continue
		}

		if info.Mode().IsDir() {
			subEntries, err := os.ReadDir(driverPath)
			if err != nil {
				continue
			}

			hasSubDirs := false
			for _, se := range subEntries {
				if se.IsDir() {
					hasSubDirs = true
					break
				}
			}

			if hasSubDirs {
				for _, versionEntry := range subEntries {
					if !versionEntry.IsDir() {
						continue
					}
					version := versionEntry.Name()
					driverDir := filepath.Join(driverPath, version, tuple)
					lib, err := findDriverLib(driverDir)
					if err != nil {
						altDir := filepath.Join(driverPath, version)
						if lib, err := findDriverLib(altDir); err == nil {
							installed = append(installed, InstalledDriver{Name: driverName, Version: version, LibPath: lib})
							continue
						}
						continue
					}
					installed = append(installed, InstalledDriver{Name: driverName, Version: version, LibPath: lib})
				}
			} else {
				lib, err := findDriverLib(driverPath)
				if err != nil {
					continue
				}
				driverName, version := extractNameAndVersion(driverName)
				installed = append(installed, InstalledDriver{Name: driverName, Version: version, LibPath: lib})
			}
		} else {
			lib, err := findDriverLib(driverPath)
			if err != nil {
				continue
			}
			installed = append(installed, InstalledDriver{Name: driverName, Version: "installed", LibPath: lib})
		}
	}

	return installed, nil
}

func extractNameAndVersion(dirName string) (string, string) {
	driverName := dirName
	version := "unknown"

	for i := len(dirName) - 1; i >= 0; i-- {
		if dirName[i] == '_' || dirName[i] == '-' {
			if i > 0 && (dirName[i-1] >= '0' && dirName[i-1] <= '9') {
				version = dirName[i+1:]
				driverName = dirName[:i]
				break
			}
		}
	}
	return driverName, version
}

func adbcDriversDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "Library", "Application Support", "ADBC", "Drivers"), nil
}

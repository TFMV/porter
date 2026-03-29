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
	platformDir := filepath.Join(cacheDir)
	driverEntries, err := os.ReadDir(platformDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read cache dir: %w", err)
	}

	var installed []InstalledDriver
	tuple := p.Tuple()
	for _, driverEntry := range driverEntries {
		if !driverEntry.IsDir() {
			continue
		}
		driverName := driverEntry.Name()
		versionsDir := filepath.Join(cacheDir, driverName)
		versionEntries, err := os.ReadDir(versionsDir)
		if err != nil {
			continue
		}

		for _, versionEntry := range versionEntries {
			if !versionEntry.IsDir() {
				continue
			}
			version := versionEntry.Name()
			driverDir := filepath.Join(versionsDir, version, tuple)
			lib, err := findDriverLib(driverDir)
			if err != nil {
				continue
			}
			installed = append(installed, InstalledDriver{Name: driverName, Version: version, LibPath: lib})
		}
	}

	sort.Slice(installed, func(i, j int) bool {
		if installed[i].Name != installed[j].Name {
			return installed[i].Name < installed[j].Name
		}
		if installed[i].Version != installed[j].Version {
			return installed[i].Version < installed[j].Version
		}
		return installed[i].LibPath < installed[j].LibPath
	})

	return installed, nil
}

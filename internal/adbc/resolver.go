package adbc

import (
	"fmt"
	"net/url"
	"os"
	"strings"
)

type Resolver struct {
	BaseURL *url.URL
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

	// canonical layout:
	// /drivers/<name>/<version>/<artifact>
	path := fmt.Sprintf(
		"drivers/%s/%s/%s_%s-%s.tar.gz",
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

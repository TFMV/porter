package ducklake

import (
	"fmt"
	"strings"
)

const (
	DefaultCatalogType = "duckdb"
	DefaultCatalogDSN  = "metadata.ducklake"
	DefaultName        = "my_ducklake"
)

type Config struct {
	Enabled     bool
	CatalogType string
	CatalogDSN  string
	DataPath    string
	Name        string
}

func (c Config) Normalize() Config {
	if !c.Enabled {
		return c
	}
	out := c
	if strings.TrimSpace(out.CatalogType) == "" {
		out.CatalogType = DefaultCatalogType
	}
	out.CatalogType = strings.ToLower(strings.TrimSpace(out.CatalogType))
	if strings.TrimSpace(out.CatalogDSN) == "" && out.CatalogType == DefaultCatalogType {
		out.CatalogDSN = DefaultCatalogDSN
	}
	if strings.TrimSpace(out.Name) == "" {
		out.Name = DefaultName
	}
	return out
}

func (c Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	cfg := c.Normalize()
	switch cfg.CatalogType {
	case "duckdb", "sqlite", "postgres", "mysql":
	default:
		return fmt.Errorf("unsupported DuckLake catalog type %q", cfg.CatalogType)
	}

	if strings.TrimSpace(cfg.CatalogDSN) == "" {
		return fmt.Errorf("DuckLake catalog DSN is required")
	}
	if strings.TrimSpace(cfg.Name) == "" {
		return fmt.Errorf("DuckLake catalog name is required")
	}
	if requiresDataPath(cfg.CatalogType) && strings.TrimSpace(cfg.DataPath) == "" {
		return fmt.Errorf("DuckLake data path is required for catalog type %q", cfg.CatalogType)
	}
	return nil
}

func (c Config) StartupSQL() ([]string, error) {
	if !c.Enabled {
		return nil, nil
	}
	cfg := c.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	sql := []string{"INSTALL ducklake;", "LOAD ducklake;"}
	for _, ext := range cfg.catalogExtensions() {
		sql = append(sql, fmt.Sprintf("INSTALL %s;", ext))
		sql = append(sql, fmt.Sprintf("LOAD %s;", ext))
	}
	sql = append(sql, cfg.attachSQL(), cfg.useSQL())
	return sql, nil
}

func (c Config) ConnectionInitSQL() ([]string, error) {
	if !c.Enabled {
		return nil, nil
	}
	cfg := c.Normalize()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	sql := []string{"LOAD ducklake;"}
	for _, ext := range cfg.catalogExtensions() {
		sql = append(sql, fmt.Sprintf("LOAD %s;", ext))
	}
	sql = append(sql, cfg.useSQL())
	return sql, nil
}

func (c Config) attachSQL() string {
	uri := c.catalogURI()
	name := quoteIdentifier(c.Name)
	params := ""
	if strings.TrimSpace(c.DataPath) != "" {
		params = fmt.Sprintf(" (DATA_PATH '%s')", quoteString(c.DataPath))
	}
	return fmt.Sprintf("ATTACH '%s' AS %s%s;", quoteString(uri), name, params)
}

func (c Config) useSQL() string {
	return fmt.Sprintf("USE %s;", quoteIdentifier(c.Name))
}

func (c Config) catalogURI() string {
	switch c.CatalogType {
	case "duckdb":
		return "ducklake:" + c.CatalogDSN
	case "sqlite":
		return "ducklake:sqlite:" + c.CatalogDSN
	case "postgres":
		return "ducklake:postgres:" + c.CatalogDSN
	case "mysql":
		return "ducklake:mysql:" + c.CatalogDSN
	default:
		return "ducklake:" + c.CatalogDSN
	}
}

func (c Config) catalogExtensions() []string {
	switch c.CatalogType {
	case "sqlite":
		return []string{"sqlite"}
	case "postgres":
		return []string{"postgres"}
	case "mysql":
		return []string{"mysql"}
	default:
		return nil
	}
}

func requiresDataPath(catalogType string) bool {
	switch catalogType {
	case "sqlite", "postgres", "mysql":
		return true
	default:
		return false
	}
}

func quoteString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func quoteIdentifier(value string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(value, `"`, `""`))
}

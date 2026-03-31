package ducklake

import (
	"strings"
	"testing"
)

func TestConfigStartupSQLBuildsCatalogSpecificAttachStrings(t *testing.T) {
	testCases := []struct {
		name      string
		cfg       Config
		wantParts []string
	}{
		{
			name: "duckdb",
			cfg: Config{
				Enabled:     true,
				CatalogType: "duckdb",
				CatalogDSN:  "metadata.ducklake",
				Name:        "my_ducklake",
			},
			wantParts: []string{
				"INSTALL ducklake;",
				"ATTACH 'ducklake:metadata.ducklake' AS \"my_ducklake\";",
			},
		},
		{
			name: "sqlite",
			cfg: Config{
				Enabled:     true,
				CatalogType: "sqlite",
				CatalogDSN:  "/tmp/catalog.sqlite",
				DataPath:    "/tmp/data",
				Name:        "lake",
			},
			wantParts: []string{
				"INSTALL sqlite;",
				"ATTACH 'ducklake:sqlite:/tmp/catalog.sqlite' AS \"lake\" (DATA_PATH '/tmp/data');",
			},
		},
		{
			name: "postgres",
			cfg: Config{
				Enabled:     true,
				CatalogType: "postgres",
				CatalogDSN:  "postgres://user:pass@host/db",
				DataPath:    "s3://bucket/path",
				Name:        "lake",
			},
			wantParts: []string{
				"INSTALL postgres;",
				"ATTACH 'ducklake:postgres:postgres://user:pass@host/db' AS \"lake\" (DATA_PATH 's3://bucket/path');",
			},
		},
		{
			name: "mysql",
			cfg: Config{
				Enabled:     true,
				CatalogType: "mysql",
				CatalogDSN:  "mysql://user:pass@host/db",
				DataPath:    "azure://container/path",
				Name:        "lake",
			},
			wantParts: []string{
				"INSTALL mysql;",
				"ATTACH 'ducklake:mysql:mysql://user:pass@host/db' AS \"lake\" (DATA_PATH 'azure://container/path');",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sql, err := tc.cfg.StartupSQL()
			if err != nil {
				t.Fatalf("StartupSQL failed: %v", err)
			}
			joined := strings.Join(sql, "\n")
			for _, want := range tc.wantParts {
				if !strings.Contains(joined, want) {
					t.Fatalf("expected startup SQL to contain %q, got %s", want, joined)
				}
			}
		})
	}
}

func TestConfigValidateRequiresDataPathForExternalCatalogs(t *testing.T) {
	err := (Config{
		Enabled:     true,
		CatalogType: "sqlite",
		CatalogDSN:  "/tmp/catalog.sqlite",
		Name:        "lake",
	}).Validate()
	if err == nil {
		t.Fatal("expected missing data path validation error")
	}
}

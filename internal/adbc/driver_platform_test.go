package adbc

import "testing"

func TestPlatformTuple_AllCases(t *testing.T) {
	cases := []struct {
		os, arch string
		want     string
	}{
		{"darwin", "arm64", "darwin_arm64"},
		{"darwin", "amd64", "darwin_amd64"},
		{"linux", "amd64", "linux_amd64"},
		{"linux", "arm64", "linux_arm64"},
	}

	for _, c := range cases {
		p := Platform{OS: c.os, Arch: c.arch}
		if got := p.Tuple(); got != c.want {
			t.Fatalf("expected %s, got %s", c.want, got)
		}
	}
}

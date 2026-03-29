package adbc

import "path/filepath"

func isDriverLib(path string) bool {
	switch filepath.Ext(path) {
	case ".so", ".dylib", ".dll":
		return true
	default:
		return false
	}
}

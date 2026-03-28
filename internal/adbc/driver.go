package adbc

import (
	"fmt"
	"runtime"
)

type Driver struct {
	Name string
}

type Platform struct {
	OS   string
	Arch string
}

func CurrentPlatform() Platform {
	return Platform{
		OS:   runtime.GOOS,
		Arch: runtime.GOARCH,
	}
}

func (p Platform) Tuple() string {
	switch {
	case p.OS == "darwin" && p.Arch == "arm64":
		return "darwin_arm64"
	case p.OS == "darwin" && p.Arch == "amd64":
		return "darwin_amd64"
	case p.OS == "linux" && p.Arch == "amd64":
		return "linux_amd64"
	case p.OS == "linux" && p.Arch == "arm64":
		return "linux_arm64"
	default:
		panic(fmt.Sprintf("unsupported platform: %s/%s", p.OS, p.Arch))
	}
}

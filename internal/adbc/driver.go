package adbc

import (
	"errors"
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
	tuple, err := p.tuple()
	if err != nil {
		panic(err)
	}
	return tuple
}

func (p Platform) tuple() (string, error) {
	switch {
	case p.OS == "darwin" && p.Arch == "arm64":
		return "darwin_arm64", nil
	case p.OS == "darwin" && p.Arch == "amd64":
		return "darwin_amd64", nil
	case p.OS == "linux" && p.Arch == "amd64":
		return "linux_amd64", nil
	case p.OS == "linux" && p.Arch == "arm64":
		return "linux_arm64", nil
	default:
		return "", fmt.Errorf("%w: %s/%s", ErrUnsupportedPlatform, p.OS, p.Arch)
	}
}

var ErrUnsupportedPlatform = errors.New("unsupported platform")

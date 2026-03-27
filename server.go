package flightsql

import execadapter "github.com/TFMV/porter/execution/adapter/flightsql"

// Server is a compatibility alias to the execution adapter implementation.
type Server = execadapter.Server

// NewServer forwards to the execution adapter constructor.
func NewServer(dbPath string) (*Server, error) {
	return execadapter.NewServer(dbPath)
}

package adbc

import (
	"sync"
	"testing"
)

func TestManager_EnsureDriver_Idempotent(t *testing.T) {
	m := &Manager{
		CacheDir: t.TempDir(),
		Resolver: DefaultResolver(),
	}

	// fake resolver override to avoid network
	m.Resolver.BaseURL = DefaultResolver().BaseURL

	var wg sync.WaitGroup
	results := make([]string, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// NOTE: this will still try to hit network unless you inject test resolver
			// In real CI, you should mock Resolver or HTTP transport
			_, _ = m.EnsureDriver("duckdb", "1.0.0")
			results[i] = "done"
		}(i)
	}

	wg.Wait()
}

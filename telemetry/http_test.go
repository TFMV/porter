package telemetry

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestStatusServerEndpoints(t *testing.T) {
	agg := NewAggregator(Config{})
	defer agg.Close()

	agg.Publish(Event{
		Component: ComponentIngress,
		Type:      TypeRequestEnd,
		Latency:   12 * time.Millisecond,
		Metadata: map[string]any{
			"path": "websocket",
		},
	})

	time.Sleep(1100 * time.Millisecond)

	server := httptest.NewServer(NewServer(agg))
	defer server.Close()

	resp, err := http.Get(server.URL + "/status/live")
	if err != nil {
		t.Fatalf("live request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected live 200, got %d", resp.StatusCode)
	}

	var snapshot Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snapshot); err != nil {
		t.Fatalf("decode live snapshot: %v", err)
	}
	if snapshot.Health.Status == "" {
		t.Fatal("expected health status in snapshot")
	}

	healthResp, err := http.Get(server.URL + "/status/health")
	if err != nil {
		t.Fatalf("health request failed: %v", err)
	}
	defer healthResp.Body.Close()

	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("expected health 200, got %d", healthResp.StatusCode)
	}

	historyResp, err := http.Get(server.URL + "/status/history")
	if err != nil {
		t.Fatalf("history request failed: %v", err)
	}
	defer historyResp.Body.Close()

	if historyResp.StatusCode != http.StatusOK {
		t.Fatalf("expected history 200, got %d", historyResp.StatusCode)
	}
}

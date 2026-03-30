package telemetry

import (
	"math"
	"testing"
	"time"
)

func TestPathSnapshotsUseRecentMinuteAndAvoidDoubleCountingRequestTotals(t *testing.T) {
	a := &Aggregator{
		components:  make(map[string]*componentSeries),
		paths:       make(map[string]*pathSeries),
		subscribers: make(map[int]chan Snapshot),
	}

	now := time.Now()
	a.record(Event{
		Time:      now.Add(-10 * time.Second),
		Component: ComponentEgress,
		Type:      TypeBatch,
		Rows:      120,
		Bytes:     1200,
		Metadata: map[string]any{
			"path":          "websocket",
			"path_terminal": true,
		},
	}, 10)
	a.record(Event{
		Time:      now.Add(-10 * time.Second),
		Component: ComponentEgress,
		Type:      TypeRequestEnd,
		Rows:      120,
		Bytes:     1200,
		Latency:   20 * time.Millisecond,
		Metadata: map[string]any{
			"path":          "websocket",
			"path_terminal": true,
		},
	}, 10)

	a.snapshot(now, 10)

	var websocket PathSnapshot
	for _, path := range a.latest.Paths {
		if path.Name == "websocket" {
			websocket = path
			break
		}
	}

	if websocket.Window != "1m" {
		t.Fatalf("expected 1m window, got %q", websocket.Window)
	}

	wantRowsPerSecond := 120.0 / 60.0
	if math.Abs(websocket.RowsPerSecond-wantRowsPerSecond) > 0.001 {
		t.Fatalf("expected %.3f rows/sec, got %.3f", wantRowsPerSecond, websocket.RowsPerSecond)
	}
}

func TestStaleQueueDepthDoesNotStickAsCurrentPressure(t *testing.T) {
	a := &Aggregator{
		components:  make(map[string]*componentSeries),
		paths:       make(map[string]*pathSeries),
		subscribers: make(map[int]chan Snapshot),
	}

	now := time.Now()
	a.record(Event{
		Time:      now.Add(-3 * time.Second),
		Component: ComponentTransport,
		Type:      TypeQueueDepth,
		Metadata: map[string]any{
			"queue_depth":    int64(64),
			"queue_capacity": int64(128),
			"queue_unit":     "bytes",
		},
	}, 10)

	a.snapshot(now, 10)

	var transport ComponentSnapshot
	for _, component := range a.latest.Components {
		if component.Name == ComponentTransport {
			transport = component
			break
		}
	}

	if transport.Queue.Current != 0 {
		t.Fatalf("expected stale queue current to reset to 0, got %d", transport.Queue.Current)
	}
	if transport.Queue.Max1m != 64 {
		t.Fatalf("expected queue history to preserve peak, got %d", transport.Queue.Max1m)
	}
}

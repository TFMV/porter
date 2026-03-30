package telemetry

import (
	"context"
	"time"
)

const (
	ComponentIngress   = "ingress"
	ComponentTransport = "transport"
	ComponentExecution = "execution"
	ComponentEgress    = "egress"
)

const (
	TypeRequestStart = "request_start"
	TypeRequestEnd   = "request_end"
	TypeBatch        = "batch"
	TypeQueueDepth   = "queue_depth"
	TypeError        = "error"
	TypeStall        = "stall"
)

type Event struct {
	Time      time.Time      `json:"time"`
	Component string         `json:"component"`
	Type      string         `json:"type"`
	Rows      int64          `json:"rows"`
	Bytes     int64          `json:"bytes"`
	Latency   time.Duration  `json:"latency"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

type Publisher interface {
	Publish(Event) bool
}

type scopeKey struct{}

func WithScope(ctx context.Context, metadata map[string]any) context.Context {
	if len(metadata) == 0 {
		return ctx
	}
	return context.WithValue(ctx, scopeKey{}, cloneMetadata(metadata))
}

func ScopedEvent(ctx context.Context, evt Event) Event {
	scoped, _ := ctx.Value(scopeKey{}).(map[string]any)
	if len(scoped) == 0 {
		if evt.Time.IsZero() {
			evt.Time = time.Now()
		}
		if evt.Metadata != nil {
			evt.Metadata = cloneMetadata(evt.Metadata)
		}
		return evt
	}

	merged := cloneMetadata(scoped)
	for k, v := range evt.Metadata {
		merged[k] = v
	}
	evt.Metadata = merged
	if evt.Time.IsZero() {
		evt.Time = time.Now()
	}
	return evt
}

func cloneMetadata(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	dst := make(map[string]any, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

package telemetry

import (
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultBufferSize   = 4096
	defaultFeedSize     = 120
	defaultHistorySize  = 180
	snapshotInterval    = time.Second
	queueWarnRatio      = 0.60
	queueCriticalRatio  = 0.85
	errorWarnRate       = 0.02
	errorCriticalRate   = 0.05
	latencyWarnMs       = 250.0
	latencyCriticalMs   = 750.0
	imbalanceWarnRatio  = 0.15
	imbalanceAlertRatio = 0.30
)

var windowDurations = map[string]time.Duration{
	"1s": time.Second,
	"5s": 5 * time.Second,
	"1m": time.Minute,
}

type Config struct {
	BufferSize  int
	FeedSize    int
	HistorySize int
}

type Aggregator struct {
	events chan Event

	dropped atomic.Uint64

	mu          sync.RWMutex
	components  map[string]*componentSeries
	paths       map[string]*pathSeries
	feed        []FeedEvent
	history     []Snapshot
	subscribers map[int]chan Snapshot
	nextSubID   int
	latest      Snapshot

	done chan struct{}
	wg   sync.WaitGroup
}

type componentSeries struct {
	activity     []activitySample
	latencies    []latencySample
	queue        []queueSample
	currentQueue queueSample
	active       int64
}

type pathSeries struct {
	activity  []activitySample
	latencies []latencySample
}

type activitySample struct {
	At     time.Time
	Rows   int64
	Bytes  int64
	Ops    int64
	Errors int64
}

type latencySample struct {
	At      time.Time
	Latency time.Duration
}

type queueSample struct {
	At       time.Time
	Depth    int64
	Capacity int64
	Unit     string
}

type Snapshot struct {
	Time          time.Time           `json:"time"`
	Components    []ComponentSnapshot `json:"components"`
	Edges         []EdgeSnapshot      `json:"edges"`
	Paths         []PathSnapshot      `json:"paths"`
	Feed          []FeedEvent         `json:"feed,omitempty"`
	Health        HealthSnapshot      `json:"health"`
	DroppedEvents uint64              `json:"dropped_events"`
}

type ComponentSnapshot struct {
	Name           string                    `json:"name"`
	Status         string                    `json:"status"`
	Pressure       string                    `json:"pressure"`
	Active         int64                     `json:"active"`
	RowsPerSecond  float64                   `json:"rows_per_second"`
	BytesPerSecond float64                   `json:"bytes_per_second"`
	ErrorRate      float64                   `json:"error_rate"`
	Latency        LatencySnapshot           `json:"latency"`
	Queue          QueueSnapshot             `json:"queue"`
	Windows        map[string]WindowSnapshot `json:"windows"`
}

type WindowSnapshot struct {
	RowsPerSecond  float64         `json:"rows_per_second"`
	BytesPerSecond float64         `json:"bytes_per_second"`
	ErrorRate      float64         `json:"error_rate"`
	Latency        LatencySnapshot `json:"latency"`
	QueueDepth     int64           `json:"queue_depth"`
	QueueCapacity  int64           `json:"queue_capacity"`
}

type LatencySnapshot struct {
	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
}

type QueueSnapshot struct {
	Current     int64   `json:"current"`
	Capacity    int64   `json:"capacity"`
	Utilization float64 `json:"utilization"`
	Max1s       int64   `json:"max_1s"`
	Max1m       int64   `json:"max_1m"`
	Unit        string  `json:"unit,omitempty"`
}

type EdgeSnapshot struct {
	From          string  `json:"from"`
	To            string  `json:"to"`
	ThroughputMBs float64 `json:"throughput_mbps"`
	Imbalance     float64 `json:"imbalance"`
	Status        string  `json:"status"`
	Pressure      string  `json:"pressure"`
}

type PathSnapshot struct {
	Name          string          `json:"name"`
	Status        string          `json:"status"`
	RowsPerSecond float64         `json:"rows_per_second"`
	MBPerSecond   float64         `json:"mb_per_second"`
	ErrorRate     float64         `json:"error_rate"`
	Latency       LatencySnapshot `json:"latency"`
	Window        string          `json:"window"`
}

type FeedEvent struct {
	Time       time.Time `json:"time"`
	Path       string    `json:"path"`
	Component  string    `json:"component"`
	Type       string    `json:"type"`
	Message    string    `json:"message"`
	Rows       int64     `json:"rows"`
	Bytes      int64     `json:"bytes"`
	LatencyMS  float64   `json:"latency_ms"`
	QueueDepth int64     `json:"queue_depth"`
}

type HealthSnapshot struct {
	Status  string   `json:"status"`
	Reasons []string `json:"reasons"`
}

func NewAggregator(cfg Config) *Aggregator {
	if cfg.BufferSize <= 0 {
		cfg.BufferSize = defaultBufferSize
	}
	if cfg.FeedSize <= 0 {
		cfg.FeedSize = defaultFeedSize
	}
	if cfg.HistorySize <= 0 {
		cfg.HistorySize = defaultHistorySize
	}

	a := &Aggregator{
		events:      make(chan Event, cfg.BufferSize),
		components:  make(map[string]*componentSeries),
		paths:       make(map[string]*pathSeries),
		subscribers: make(map[int]chan Snapshot),
		done:        make(chan struct{}),
		feed:        make([]FeedEvent, 0, cfg.FeedSize),
		history:     make([]Snapshot, 0, cfg.HistorySize),
		latest:      Snapshot{Time: time.Now(), Health: HealthSnapshot{Status: "healthy"}},
	}

	a.wg.Add(1)
	go a.run(cfg.FeedSize, cfg.HistorySize)
	return a
}

func (a *Aggregator) Close() {
	close(a.done)
	a.wg.Wait()
}

func (a *Aggregator) Publish(evt Event) bool {
	if evt.Time.IsZero() {
		evt.Time = time.Now()
	}
	evt.Metadata = cloneMetadata(evt.Metadata)

	select {
	case a.events <- evt:
		return true
	default:
		a.dropped.Add(1)
		return false
	}
}

func (a *Aggregator) Snapshot() Snapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return cloneSnapshot(a.latest)
}

func (a *Aggregator) History() []Snapshot {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]Snapshot, len(a.history))
	for i := range a.history {
		out[i] = cloneSnapshot(a.history[i])
	}
	return out
}

func (a *Aggregator) Subscribe() (<-chan Snapshot, func()) {
	a.mu.Lock()
	defer a.mu.Unlock()

	id := a.nextSubID
	a.nextSubID++
	ch := make(chan Snapshot, 1)
	ch <- cloneSnapshot(a.latest)
	a.subscribers[id] = ch

	cancel := func() {
		a.mu.Lock()
		defer a.mu.Unlock()
		if sub, ok := a.subscribers[id]; ok {
			delete(a.subscribers, id)
			close(sub)
		}
	}
	return ch, cancel
}

func (a *Aggregator) run(feedSize, historySize int) {
	defer a.wg.Done()

	ticker := time.NewTicker(snapshotInterval)
	defer ticker.Stop()

	for {
		select {
		case evt := <-a.events:
			a.record(evt, feedSize)
		case now := <-ticker.C:
			a.snapshot(now, historySize)
		case <-a.done:
			a.snapshot(time.Now(), historySize)
			return
		}
	}
}

func (a *Aggregator) record(evt Event, feedSize int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	component := a.component(evt.Component)
	if shouldIncrementActive(evt.Type) {
		component.active++
	}
	if shouldDecrementActive(evt.Type) && component.active > 0 {
		component.active--
	}

	if evt.Rows != 0 || evt.Bytes != 0 || evt.Type == TypeRequestEnd || evt.Type == TypeError {
		component.activity = append(component.activity, activitySample{
			At:     evt.Time,
			Rows:   evt.Rows,
			Bytes:  evt.Bytes,
			Ops:    opCount(evt),
			Errors: errorCount(evt),
		})
	}

	if evt.Latency > 0 {
		component.latencies = append(component.latencies, latencySample{
			At:      evt.Time,
			Latency: evt.Latency,
		})
	}

	if evt.Type == TypeQueueDepth {
		sample := queueSample{
			At:       evt.Time,
			Depth:    metadataInt(evt.Metadata, "queue_depth"),
			Capacity: metadataInt(evt.Metadata, "queue_capacity"),
			Unit:     metadataString(evt.Metadata, "queue_unit"),
		}
		component.queue = append(component.queue, sample)
		component.currentQueue = sample
	}

	if isTerminalPathEvent(evt) {
		pathName := metadataString(evt.Metadata, "path")
		if pathName == "" {
			pathName = "unknown"
		}
		path := a.path(pathName)
		recordThroughput := evt.Type == TypeBatch || metadataBool(evt.Metadata, "path_summary")
		pathRows := int64(0)
		pathBytes := int64(0)
		if recordThroughput {
			pathRows = evt.Rows
			pathBytes = evt.Bytes
		}
		if pathRows != 0 || pathBytes != 0 || evt.Type == TypeRequestEnd || evt.Type == TypeError {
			path.activity = append(path.activity, activitySample{
				At:     evt.Time,
				Rows:   pathRows,
				Bytes:  pathBytes,
				Ops:    opCount(evt),
				Errors: errorCount(evt),
			})
		}
		if evt.Latency > 0 {
			path.latencies = append(path.latencies, latencySample{
				At:      evt.Time,
				Latency: evt.Latency,
			})
		}
	}

	if feedEvent, ok := renderFeedEvent(evt); ok {
		a.feed = append(a.feed, feedEvent)
		if len(a.feed) > feedSize {
			a.feed = append([]FeedEvent(nil), a.feed[len(a.feed)-feedSize:]...)
		}
	}
}

func (a *Aggregator) snapshot(now time.Time, historySize int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	cutoff := now.Add(-time.Minute)
	for _, component := range a.components {
		component.activity = trimActivity(component.activity, cutoff)
		component.latencies = trimLatencies(component.latencies, cutoff)
		component.queue = trimQueue(component.queue, cutoff)
	}
	for _, path := range a.paths {
		path.activity = trimActivity(path.activity, cutoff)
		path.latencies = trimLatencies(path.latencies, cutoff)
	}

	snapshot := Snapshot{
		Time:          now,
		Components:    a.componentSnapshots(now),
		Paths:         a.pathSnapshots(now),
		Feed:          slices.Clone(a.feed),
		DroppedEvents: a.dropped.Load(),
	}
	snapshot.Edges = buildEdges(snapshot.Components)
	snapshot.Health = evaluateHealth(snapshot.Components)

	a.latest = snapshot

	historyPoint := cloneSnapshot(snapshot)
	historyPoint.Feed = nil
	a.history = append(a.history, historyPoint)
	if len(a.history) > historySize {
		a.history = append([]Snapshot(nil), a.history[len(a.history)-historySize:]...)
	}

	for id, sub := range a.subscribers {
		select {
		case sub <- cloneSnapshot(snapshot):
		default:
			select {
			case <-sub:
			default:
			}
			select {
			case sub <- cloneSnapshot(snapshot):
			default:
				delete(a.subscribers, id)
				close(sub)
			}
		}
	}
}

func (a *Aggregator) component(name string) *componentSeries {
	if a.components[name] == nil {
		a.components[name] = &componentSeries{}
	}
	return a.components[name]
}

func (a *Aggregator) path(name string) *pathSeries {
	if a.paths[name] == nil {
		a.paths[name] = &pathSeries{}
	}
	return a.paths[name]
}

func (a *Aggregator) componentSnapshots(now time.Time) []ComponentSnapshot {
	names := []string{ComponentIngress, ComponentTransport, ComponentExecution, ComponentEgress}
	out := make([]ComponentSnapshot, 0, len(names))
	for _, name := range names {
		series := a.components[name]
		if series == nil {
			out = append(out, ComponentSnapshot{
				Name:     name,
				Status:   "healthy",
				Pressure: "stable",
				Windows:  make(map[string]WindowSnapshot, len(windowDurations)),
			})
			continue
		}

		windows := make(map[string]WindowSnapshot, len(windowDurations))
		for label, dur := range windowDurations {
			windows[label] = summarizeWindow(series.activity, series.latencies, series.queue, now, dur)
		}
		oneSecond := windows["1s"]
		oneMinute := windows["1m"]
		currentQueue := series.currentQueue
		if currentQueue.At.IsZero() || now.Sub(currentQueue.At) > (2*snapshotInterval) {
			currentQueue.Depth = 0
		}
		queue := QueueSnapshot{
			Current:     currentQueue.Depth,
			Capacity:    currentQueue.Capacity,
			Utilization: ratio(currentQueue.Depth, currentQueue.Capacity),
			Max1s:       oneSecond.QueueDepth,
			Max1m:       oneMinute.QueueDepth,
			Unit:        currentQueue.Unit,
		}
		status, pressure := pressureForComponent(oneSecond, queue)
		out = append(out, ComponentSnapshot{
			Name:           name,
			Status:         status,
			Pressure:       pressure,
			Active:         series.active,
			RowsPerSecond:  oneSecond.RowsPerSecond,
			BytesPerSecond: oneSecond.BytesPerSecond,
			ErrorRate:      oneSecond.ErrorRate,
			Latency:        oneSecond.Latency,
			Queue:          queue,
			Windows:        windows,
		})
	}
	return out
}

func (a *Aggregator) pathSnapshots(now time.Time) []PathSnapshot {
	names := []string{"websocket", "flightsql", "ingest"}
	out := make([]PathSnapshot, 0, len(names))
	for _, name := range names {
		series := a.paths[name]
		if series == nil {
			out = append(out, PathSnapshot{Name: name, Status: "healthy", Window: "1s"})
			continue
		}
		window := summarizeWindow(series.activity, series.latencies, nil, now, time.Minute)
		status := "healthy"
		if window.ErrorRate >= errorCriticalRate || window.Latency.P99MS >= latencyCriticalMs {
			status = "degraded"
		} else if window.ErrorRate >= errorWarnRate || window.Latency.P99MS >= latencyWarnMs {
			status = "warning"
		}
		out = append(out, PathSnapshot{
			Name:          name,
			Status:        status,
			RowsPerSecond: window.RowsPerSecond,
			MBPerSecond:   window.BytesPerSecond / (1024 * 1024),
			ErrorRate:     window.ErrorRate,
			Latency:       window.Latency,
			Window:        "1m",
		})
	}
	return out
}

func summarizeWindow(activity []activitySample, latencies []latencySample, queue []queueSample, now time.Time, window time.Duration) WindowSnapshot {
	start := now.Add(-window)
	var rows, bytes, ops, errors int64
	for _, sample := range activity {
		if sample.At.Before(start) {
			continue
		}
		rows += sample.Rows
		bytes += sample.Bytes
		ops += sample.Ops
		errors += sample.Errors
	}
	windowSecs := window.Seconds()
	latSummary := summarizeLatencies(latencies, start)
	var maxQueue int64
	var queueCapacity int64
	for _, sample := range queue {
		if sample.At.Before(start) {
			continue
		}
		if sample.Depth > maxQueue {
			maxQueue = sample.Depth
		}
		if sample.Capacity > queueCapacity {
			queueCapacity = sample.Capacity
		}
	}
	return WindowSnapshot{
		RowsPerSecond:  float64(rows) / windowSecs,
		BytesPerSecond: float64(bytes) / windowSecs,
		ErrorRate:      ratio(errors, maxInt64(ops, 1)),
		Latency:        latSummary,
		QueueDepth:     maxQueue,
		QueueCapacity:  queueCapacity,
	}
}

func summarizeLatencies(samples []latencySample, cutoff time.Time) LatencySnapshot {
	values := make([]float64, 0, len(samples))
	for _, sample := range samples {
		if sample.At.Before(cutoff) {
			continue
		}
		values = append(values, float64(sample.Latency)/float64(time.Millisecond))
	}
	if len(values) == 0 {
		return LatencySnapshot{}
	}
	slices.Sort(values)
	return LatencySnapshot{
		P50MS: percentile(values, 0.50),
		P95MS: percentile(values, 0.95),
		P99MS: percentile(values, 0.99),
	}
}

func buildEdges(components []ComponentSnapshot) []EdgeSnapshot {
	component := make(map[string]ComponentSnapshot, len(components))
	for _, item := range components {
		component[item.Name] = item
	}

	ingress := component[ComponentIngress]
	transport := component[ComponentTransport]
	execution := component[ComponentExecution]
	egress := component[ComponentEgress]

	return []EdgeSnapshot{
		edgeBetween(ingress, transport, execution, ComponentIngress, ComponentTransport),
		edgeBetween(transport, execution, egress, ComponentTransport, ComponentExecution),
		edgeBetween(execution, egress, ComponentSnapshot{}, ComponentExecution, ComponentEgress),
	}
}

func edgeBetween(upstream, downstream, fallback ComponentSnapshot, from, to string) EdgeSnapshot {
	throughput := downstream.BytesPerSecond
	if throughput == 0 {
		throughput = upstream.BytesPerSecond
	}
	upRows := upstream.RowsPerSecond
	downRows := downstream.RowsPerSecond
	if downRows == 0 && fallback.RowsPerSecond > 0 {
		downRows = fallback.RowsPerSecond
	}
	imbalance := rateMismatch(upRows, downRows)
	queueUtil := downstream.Queue.Utilization
	if queueUtil == 0 {
		queueUtil = fallback.Queue.Utilization
	}
	status, pressure := pressureForEdge(imbalance, queueUtil)
	return EdgeSnapshot{
		From:          from,
		To:            to,
		ThroughputMBs: throughput / (1024 * 1024),
		Imbalance:     imbalance,
		Status:        status,
		Pressure:      pressure,
	}
}

func evaluateHealth(components []ComponentSnapshot) HealthSnapshot {
	reasons := make([]string, 0, 4)
	for _, component := range components {
		if component.Queue.Utilization >= queueWarnRatio {
			reasons = append(reasons, component.Name+"_backpressure")
		}
		if component.Latency.P99MS >= latencyWarnMs {
			reasons = append(reasons, component.Name+"_p99_latency_high")
		}
		if component.ErrorRate >= errorWarnRate {
			reasons = append(reasons, component.Name+"_error_rate_high")
		}
	}
	if len(reasons) == 0 {
		return HealthSnapshot{Status: "healthy"}
	}
	return HealthSnapshot{Status: "degraded", Reasons: reasons}
}

func pressureForComponent(window WindowSnapshot, queue QueueSnapshot) (string, string) {
	switch {
	case queue.Utilization >= queueCriticalRatio || window.ErrorRate >= errorCriticalRate || window.Latency.P99MS >= latencyCriticalMs:
		return "degraded", "critical"
	case queue.Utilization >= queueWarnRatio || window.ErrorRate >= errorWarnRate || window.Latency.P99MS >= latencyWarnMs:
		return "warning", "building"
	default:
		return "healthy", "stable"
	}
}

func pressureForEdge(imbalance, queueUtil float64) (string, string) {
	switch {
	case imbalance >= imbalanceAlertRatio || queueUtil >= queueCriticalRatio:
		return "degraded", "high"
	case imbalance >= imbalanceWarnRatio || queueUtil >= queueWarnRatio:
		return "warning", "rising"
	default:
		return "healthy", "balanced"
	}
}

func renderFeedEvent(evt Event) (FeedEvent, bool) {
	if evt.Type == TypeRequestStart {
		return FeedEvent{}, false
	}
	path := metadataString(evt.Metadata, "path")
	queueDepth := metadataInt(evt.Metadata, "queue_depth")
	message := describeEvent(evt, path, queueDepth)
	if message == "" {
		return FeedEvent{}, false
	}
	return FeedEvent{
		Time:       evt.Time,
		Path:       path,
		Component:  evt.Component,
		Type:       evt.Type,
		Message:    message,
		Rows:       evt.Rows,
		Bytes:      evt.Bytes,
		LatencyMS:  float64(evt.Latency) / float64(time.Millisecond),
		QueueDepth: queueDepth,
	}, true
}

func describeEvent(evt Event, path string, queueDepth int64) string {
	switch evt.Type {
	case TypeBatch:
		if evt.Rows == 0 {
			return ""
		}
		return fmt.Sprintf("%s batch %s rows (%s)", labelFor(path, evt.Component), formatInt(evt.Rows), formatDuration(evt.Latency))
	case TypeQueueDepth:
		if queueDepth == 0 {
			return ""
		}
		return fmt.Sprintf("%s pressure building (queue=%s)", labelFor(path, evt.Component), formatInt(queueDepth))
	case TypeStall:
		return fmt.Sprintf("%s stall detected (%s)", labelFor(path, evt.Component), formatDuration(evt.Latency))
	case TypeError:
		if reason := metadataString(evt.Metadata, "reason"); reason != "" {
			return fmt.Sprintf("%s error: %s", labelFor(path, evt.Component), reason)
		}
		return fmt.Sprintf("%s error detected", labelFor(path, evt.Component))
	case TypeRequestEnd:
		if evt.Rows > 0 {
			return fmt.Sprintf("%s completed %s rows in %s", labelFor(path, evt.Component), formatInt(evt.Rows), formatDuration(evt.Latency))
		}
		if evt.Latency > 0 {
			return fmt.Sprintf("%s completed in %s", labelFor(path, evt.Component), formatDuration(evt.Latency))
		}
	}
	return ""
}

func labelFor(path, component string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return component
	}
	return path
}

func cloneSnapshot(snapshot Snapshot) Snapshot {
	out := snapshot
	out.Components = slices.Clone(snapshot.Components)
	for i := range out.Components {
		if out.Components[i].Windows != nil {
			windows := make(map[string]WindowSnapshot, len(out.Components[i].Windows))
			for key, val := range out.Components[i].Windows {
				windows[key] = val
			}
			out.Components[i].Windows = windows
		}
	}
	out.Edges = slices.Clone(snapshot.Edges)
	out.Paths = slices.Clone(snapshot.Paths)
	out.Feed = slices.Clone(snapshot.Feed)
	out.Health.Reasons = slices.Clone(snapshot.Health.Reasons)
	return out
}

func trimActivity(samples []activitySample, cutoff time.Time) []activitySample {
	idx := 0
	for idx < len(samples) && samples[idx].At.Before(cutoff) {
		idx++
	}
	return append([]activitySample(nil), samples[idx:]...)
}

func trimLatencies(samples []latencySample, cutoff time.Time) []latencySample {
	idx := 0
	for idx < len(samples) && samples[idx].At.Before(cutoff) {
		idx++
	}
	return append([]latencySample(nil), samples[idx:]...)
}

func trimQueue(samples []queueSample, cutoff time.Time) []queueSample {
	idx := 0
	for idx < len(samples) && samples[idx].At.Before(cutoff) {
		idx++
	}
	return append([]queueSample(nil), samples[idx:]...)
}

func shouldIncrementActive(eventType string) bool {
	return eventType == TypeRequestStart
}

func shouldDecrementActive(eventType string) bool {
	return eventType == TypeRequestEnd || eventType == TypeError
}

func isTerminalPathEvent(evt Event) bool {
	return metadataBool(evt.Metadata, "path_terminal")
}

func opCount(evt Event) int64 {
	if evt.Type == TypeRequestEnd || evt.Type == TypeError {
		return 1
	}
	return 0
}

func errorCount(evt Event) int64 {
	if evt.Type == TypeError {
		return 1
	}
	return 0
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if len(sorted) == 1 {
		return sorted[0]
	}
	index := p * float64(len(sorted)-1)
	lower := int(math.Floor(index))
	upper := int(math.Ceil(index))
	if lower == upper {
		return sorted[lower]
	}
	weight := index - float64(lower)
	return sorted[lower] + (sorted[upper]-sorted[lower])*weight
}

func ratio(value, total int64) float64 {
	if total <= 0 {
		return 0
	}
	return float64(value) / float64(total)
}

func rateMismatch(upstream, downstream float64) float64 {
	if upstream <= 0 {
		return 0
	}
	if downstream >= upstream {
		return 0
	}
	return (upstream - downstream) / upstream
}

func metadataString(metadata map[string]any, key string) string {
	if len(metadata) == 0 {
		return ""
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return fmt.Sprint(value)
	}
}

func metadataInt(metadata map[string]any, key string) int64 {
	if len(metadata) == 0 {
		return 0
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return 0
	}
	switch typed := value.(type) {
	case int:
		return int64(typed)
	case int32:
		return int64(typed)
	case int64:
		return typed
	case uint:
		return int64(typed)
	case uint32:
		return int64(typed)
	case uint64:
		return int64(typed)
	case float64:
		return int64(typed)
	case string:
		n, err := strconv.ParseInt(typed, 10, 64)
		if err == nil {
			return n
		}
	}
	return 0
}

func metadataBool(metadata map[string]any, key string) bool {
	if len(metadata) == 0 {
		return false
	}
	value, ok := metadata[key]
	if !ok || value == nil {
		return false
	}
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(typed, "true")
	default:
		return false
	}
}

func formatInt(value int64) string {
	negative := value < 0
	if negative {
		value = -value
	}
	digits := strconv.FormatInt(value, 10)
	if len(digits) <= 3 {
		if negative {
			return "-" + digits
		}
		return digits
	}
	var builder strings.Builder
	if negative {
		builder.WriteByte('-')
	}
	offset := len(digits) % 3
	if offset == 0 {
		offset = 3
	}
	builder.WriteString(digits[:offset])
	for i := offset; i < len(digits); i += 3 {
		builder.WriteByte(',')
		builder.WriteString(digits[i : i+3])
	}
	return builder.String()
}

func formatDuration(duration time.Duration) string {
	if duration <= 0 {
		return "0ms"
	}
	if duration < time.Millisecond {
		return duration.String()
	}
	return fmt.Sprintf("%.0fms", float64(duration)/float64(time.Millisecond))
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

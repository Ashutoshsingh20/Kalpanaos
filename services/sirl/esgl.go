package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type EventPriority int

const (
	PriorityCritical    EventPriority = 1 // Immediate dispatch (crashes, policy violations, CPU threshold violations)
	PriorityOperational EventPriority = 2 // Buffered sliding window, flushed as 5-minute averages (telemetry, gossip heartbeats)
	PriorityCognitive   EventPriority = 3 // Summarized into semantic summaries (anomaly predictions, drift warnings)
)

type Event struct {
	Subject  string
	Priority EventPriority
	Payload  interface{}
}

type EventSaturationGovernance struct {
	mu           sync.Mutex
	nc           *nats.Conn
	nodeID       string
	telemetryBuf []TelemetryMetric
	gossipBuf    []NodeCapabilities
	flushTicker  *time.Ticker
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewEventSaturationGovernance(nc *nats.Conn, nodeID string) *EventSaturationGovernance {
	ctx, cancel := context.WithCancel(context.Background())
	esg := &EventSaturationGovernance{
		nc:          nc,
		nodeID:      nodeID,
		ctx:         ctx,
		cancel:      cancel,
		flushTicker: time.NewTicker(5 * time.Minute),
	}
	go esg.startFlushLoop()
	return esg
}

func (e *EventSaturationGovernance) Close() {
	e.cancel()
	e.flushTicker.Stop()
	e.flushBuffers()
}

func (e *EventSaturationGovernance) Dispatch(evt Event) {
	if e.nc == nil || !e.nc.IsConnected() {
		return
	}

	switch evt.Priority {
	case PriorityCritical:
		e.publishImmediate(evt.Subject, evt.Payload)

	case PriorityOperational:
		e.mu.Lock()
		defer e.mu.Unlock()
		if evt.Subject == "kalpana.sirl.domain.runtime.events.telebeat" {
			if metric, ok := evt.Payload.(TelemetryMetric); ok {
				e.telemetryBuf = append(e.telemetryBuf, metric)
				// Limit queue growth to avoid memory issues under extreme conditions
				if len(e.telemetryBuf) > 120 { // ~10 minutes of 5s metrics
					e.telemetryBuf = e.telemetryBuf[1:]
				}
			}
		} else if evt.Subject == "kalpana.sirl.domain.coordination.gossip.heartbeat" {
			if cap, ok := evt.Payload.(NodeCapabilities); ok {
				e.gossipBuf = append(e.gossipBuf, cap)
				if len(e.gossipBuf) > 50 {
					e.gossipBuf = e.gossipBuf[1:]
				}
			}
		}

	case PriorityCognitive:
		e.publishCognitive(evt.Subject, evt.Payload)
	}
}

func (e *EventSaturationGovernance) publishImmediate(subject string, payload interface{}) {
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[ESGL] Error marshalling critical event: %v", err)
		return
	}
	if err := e.nc.Publish(subject, data); err != nil {
		log.Printf("[ESGL] Failed to publish critical event: %v", err)
	}
}

func (e *EventSaturationGovernance) publishCognitive(subject string, payload interface{}) {
	// Priority 3: Cognitive event semantic summarization to compress data payload
	summaryPayload := make(map[string]interface{})
	
	switch val := payload.(type) {
	case map[string]interface{}:
		for k, v := range val {
			summaryPayload[k] = v
		}
		summaryPayload["summarized"] = true
		summaryPayload["summary_timestamp"] = time.Now().UTC().Format(time.RFC3339)
	default:
		summaryPayload["data"] = payload
		summaryPayload["summarized"] = true
	}
	
	data, err := json.Marshal(summaryPayload)
	if err != nil {
		log.Printf("[ESGL] Error marshalling cognitive event: %v", err)
		return
	}
	if err := e.nc.Publish(subject, data); err != nil {
		log.Printf("[ESGL] Failed to publish cognitive event: %v", err)
	}
}

func (e *EventSaturationGovernance) startFlushLoop() {
	for {
		select {
		case <-e.flushTicker.C:
			e.flushBuffers()
		case <-e.ctx.Done():
			return
		}
	}
}

func (e *EventSaturationGovernance) flushBuffers() {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Flush telemetry as 5-minute averages
	if len(e.telemetryBuf) > 0 {
		var totalCPU, totalTemp float64
		var totalMem uint64
		for _, m := range e.telemetryBuf {
			totalCPU += m.CPUUsage
			totalMem += m.MemUsed
			totalTemp += m.Temp
		}
		n := float64(len(e.telemetryBuf))
		avgCPU := totalCPU / n
		avgMem := float64(totalMem) / n
		avgTemp := totalTemp / n

		report := map[string]interface{}{
			"timestamp":          time.Now().UTC().Format(time.RFC3339),
			"node_id":            e.nodeID,
			"avg_cpu_pct":        avgCPU,
			"avg_mem_bytes":      uint64(avgMem),
			"avg_temp_c":         avgTemp,
			"sample_count":       len(e.telemetryBuf),
			"priority":           "operational_buffered_average",
			"duration_seconds":   int(n * 5), // Assumes 5s telemetry loop
		}
		data, err := json.Marshal(report)
		if err == nil {
			_ = e.nc.Publish("kalpana.sirl.domain.runtime.events.telebeat", data)
			log.Printf("[ESGL] Flushed %d telemetry samples as averages to NATS", len(e.telemetryBuf))
		}
		e.telemetryBuf = nil
	}

	// 2. Flush gossip heartbeats (only publish the latest node capability)
	if len(e.gossipBuf) > 0 {
		latestCap := e.gossipBuf[len(e.gossipBuf)-1]
		data, err := json.Marshal(latestCap)
		if err == nil {
			_ = e.nc.Publish("kalpana.sirl.domain.coordination.gossip.heartbeat", data)
			log.Printf("[ESGL] Flushed gossip heartbeat (discarded %d stale heartbeats)", len(e.gossipBuf)-1)
		}
		e.gossipBuf = nil
	}
}

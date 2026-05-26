package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type DECEEvent struct {
	ID        string          `json:"id"`
	Epoch     int64           `json:"epoch"`
	Lamport   int64           `json:"lamport"`
	NodeID    string          `json:"node_id"`
	Subject   string          `json:"subject"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp time.Time       `json:"timestamp"`
}

type DeterministicEventCoordinationEngine struct {
	mu           sync.Mutex
	nc           *nats.Conn
	nodeID       string
	epoch        int64
	lamport      int64
	delayQueue   []DECEEvent
	subscribers  map[string]func(DECEEvent)
	delayWindow  time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
}

func NewDeterministicEventCoordinationEngine(nc *nats.Conn, nodeID string) *DeterministicEventCoordinationEngine {
	ctx, cancel := context.WithCancel(context.Background())
	dece := &DeterministicEventCoordinationEngine{
		nc:          nc,
		nodeID:      nodeID,
		epoch:       1,
		lamport:     0,
		subscribers: make(map[string]func(DECEEvent)),
		delayWindow: 500 * time.Millisecond,
		ctx:         ctx,
		cancel:      cancel,
	}
	go dece.startDelayQueueProcessor()
	return dece
}

func (d *DeterministicEventCoordinationEngine) Close() {
	d.cancel()
}

func (d *DeterministicEventCoordinationEngine) IncrementLamport() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.lamport++
	return d.lamport
}

func (d *DeterministicEventCoordinationEngine) UpdateLamport(received int64) int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if received > d.lamport {
		d.lamport = received
	}
	d.lamport++
	return d.lamport
}

func (d *DeterministicEventCoordinationEngine) PublishEvent(subject string, payload interface{}) error {
	if d.nc == nil {
		return fmt.Errorf("NATS not connected")
	}

	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	d.mu.Lock()
	d.lamport++
	evt := DECEEvent{
		ID:        fmt.Sprintf("evt-%s-%d", d.nodeID, time.Now().UnixNano()),
		Epoch:     d.epoch,
		Lamport:   d.lamport,
		NodeID:    d.nodeID,
		Subject:   subject,
		Payload:   rawPayload,
		Timestamp: time.Now(),
	}
	d.mu.Unlock()

	data, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	return d.nc.Publish(subject, data)
}

func (d *DeterministicEventCoordinationEngine) SubscribeEvent(subject string, handler func(DECEEvent)) error {
	if d.nc == nil {
		return fmt.Errorf("NATS not connected")
	}

	d.mu.Lock()
	d.subscribers[subject] = handler
	d.mu.Unlock()

	_, err := d.nc.Subscribe(subject, func(msg *nats.Msg) {
		var evt DECEEvent
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			log.Printf("[DECE] Error unmarshalling coordination event: %v", err)
			return
		}

		// Update Lamport clock
		d.UpdateLamport(evt.Lamport)

		// Queue event for deterministic delay processing
		d.mu.Lock()
		d.delayQueue = append(d.delayQueue, evt)
		d.mu.Unlock()
	})

	return err
}

func (d *DeterministicEventCoordinationEngine) startDelayQueueProcessor() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.processQueue()
		case <-d.ctx.Done():
			return
		}
	}
}

func (d *DeterministicEventCoordinationEngine) processQueue() {
	d.mu.Lock()
	if len(d.delayQueue) == 0 {
		d.mu.Unlock()
		return
	}

	now := time.Now()
	var readyToProcess []DECEEvent
	var remaining []DECEEvent

	for _, evt := range d.delayQueue {
		if now.Sub(evt.Timestamp) >= d.delayWindow {
			readyToProcess = append(readyToProcess, evt)
		} else {
			remaining = append(remaining, evt)
		}
	}
	d.delayQueue = remaining
	d.mu.Unlock()

	if len(readyToProcess) == 0 {
		return
	}

	// Deterministic Sort: Sort by Epoch, then Lamport, then NodeID (tie-breaker)
	for i := 0; i < len(readyToProcess); i++ {
		for j := i + 1; j < len(readyToProcess); j++ {
			swap := false
			if readyToProcess[i].Epoch > readyToProcess[j].Epoch {
				swap = true
			} else if readyToProcess[i].Epoch == readyToProcess[j].Epoch {
				if readyToProcess[i].Lamport > readyToProcess[j].Lamport {
					swap = true
				} else if readyToProcess[i].Lamport == readyToProcess[j].Lamport {
					if readyToProcess[i].NodeID > readyToProcess[j].NodeID {
						swap = true
					}
				}
			}
			if swap {
				readyToProcess[i], readyToProcess[j] = readyToProcess[j], readyToProcess[i]
			}
		}
	}

	// Dispatch sorted events
	for _, evt := range readyToProcess {
		d.mu.Lock()
		handler, exists := d.subscribers[evt.Subject]
		d.mu.Unlock()

		if exists {
			handler(evt)
		}
	}
}

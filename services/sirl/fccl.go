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

type FCCLReconciliation struct {
	ElementID    string    `json:"element_id"`
	ElementType  string    `json:"element_type"` // node | edge
	Payload      string    `json:"payload"`
	VectorClock  string    `json:"vector_clock"` // e.g. {"node1":5,"node2":3}
	Tombstone    int       `json:"tombstone"`
	LastModified time.Time `json:"last_modified"`
}

type FederatedCognitionConsistencyLayer struct {
	mu      sync.Mutex
	nc      *nats.Conn
	nodeID  string
	storage *SegmentedStorage
	clocks  map[string]int64
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewFederatedCognitionConsistencyLayer(nc *nats.Conn, nodeID string, storage *SegmentedStorage) *FederatedCognitionConsistencyLayer {
	ctx, cancel := context.WithCancel(context.Background())
	fccl := &FederatedCognitionConsistencyLayer{
		nc:      nc,
		nodeID:  nodeID,
		storage: storage,
		clocks:  make(map[string]int64),
		ctx:     ctx,
		cancel:  cancel,
	}
	fccl.clocks[nodeID] = 0
	return fccl
}

func (f *FederatedCognitionConsistencyLayer) Start() {
	if f.nc == nil {
		return
	}

	// Subscribe to reconciliation events
	_, err := f.nc.Subscribe("kalpana.sirl.domain.coordination.fccl.sync", func(msg *nats.Msg) {
		var elements []FCCLReconciliation
		if err := json.Unmarshal(msg.Data, &elements); err != nil {
			return
		}
		f.reconcileState(elements)
	})
	if err != nil {
		log.Printf("[FCCL] Sync subscription failed: %v", err)
	}

	go f.startSyncLoop()
}

func (f *FederatedCognitionConsistencyLayer) Close() {
	f.cancel()
}

func (f *FederatedCognitionConsistencyLayer) LocalClockTick() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.clocks[f.nodeID]++
	return f.serializeVectorClock()
}

func (f *FederatedCognitionConsistencyLayer) serializeVectorClock() string {
	b, _ := json.Marshal(f.clocks)
	return string(b)
}

func (f *FederatedCognitionConsistencyLayer) startSyncLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.broadcastSyncPayload()
		case <-f.ctx.Done():
			return
		}
	}
}

func (f *FederatedCognitionConsistencyLayer) broadcastSyncPayload() {
	if f.nc == nil || !f.nc.IsConnected() {
		return
	}

	// Gather graph nodes from cognition.db
	rows, err := f.storage.CognitionDB.Query("SELECT id, type, detail, timestamp FROM graph_nodes LIMIT 100")
	if err != nil {
		return
	}
	defer rows.Close()

	var elements []FCCLReconciliation
	for rows.Next() {
		var id, t, detail string
		var ts time.Time
		if err := rows.Scan(&id, &t, &detail, &ts); err == nil {
			elements = append(elements, FCCLReconciliation{
				ElementID:    id,
				ElementType:  "node",
				Payload:      fmt.Sprintf("%s:%s", t, detail),
				VectorClock:  f.LocalClockTick(),
				Tombstone:    0,
				LastModified: ts,
			})
		}
	}

	if len(elements) == 0 {
		return
	}

	data, _ := json.Marshal(elements)
	_ = f.nc.Publish("kalpana.sirl.domain.coordination.fccl.sync", data)
}

func (f *FederatedCognitionConsistencyLayer) reconcileState(elements []FCCLReconciliation) {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, el := range elements {
		if el.ElementType == "node" {
			// Check if node already exists
			var existingTS time.Time
			err := f.storage.CognitionDB.QueryRow("SELECT timestamp FROM graph_nodes WHERE id = ?", el.ElementID).Scan(&existingTS)
			if err != nil {
				// Node doesn't exist, insert it!
				parts := fmt.Sprintf("Reconciliation: %s", el.Payload)
				_, _ = f.storage.CognitionDB.Exec(`
					INSERT OR REPLACE INTO graph_nodes (id, type, detail, timestamp)
					VALUES (?, 'memory', ?, ?)
				`, el.ElementID, parts, el.LastModified)
			} else if el.LastModified.After(existingTS) {
				// Remote has a newer state, update it!
				parts := fmt.Sprintf("Reconciliation Update: %s", el.Payload)
				_, _ = f.storage.CognitionDB.Exec(`
					UPDATE graph_nodes SET detail = ?, timestamp = ? WHERE id = ?
				`, parts, el.LastModified, el.ElementID)
			}
		}
	}
}

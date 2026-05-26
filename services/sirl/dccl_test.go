package main

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// Helper to initialize in-memory SegmentedStorage for testing
func initTestSegmentedStorage(t *testing.T) *SegmentedStorage {
	uid := time.Now().UnixNano()
	r, err := sql.Open("sqlite3", fmt.Sprintf("file:runtime_%d?mode=memory&cache=shared", uid))
	if err != nil {
		t.Fatalf("failed to open runtime db: %v", err)
	}
	tel, err := sql.Open("sqlite3", fmt.Sprintf("file:telemetry_%d?mode=memory&cache=shared", uid))
	if err != nil {
		r.Close()
		t.Fatalf("failed to open telemetry db: %v", err)
	}
	g, err := sql.Open("sqlite3", fmt.Sprintf("file:governance_%d?mode=memory&cache=shared", uid))
	if err != nil {
		r.Close()
		tel.Close()
		t.Fatalf("failed to open governance db: %v", err)
	}
	c, err := sql.Open("sqlite3", fmt.Sprintf("file:cognition_%d?mode=memory&cache=shared", uid))
	if err != nil {
		r.Close()
		tel.Close()
		g.Close()
		t.Fatalf("failed to open cognition db: %v", err)
	}

	if err := runMigrations(r, tel, g, c); err != nil {
		r.Close()
		tel.Close()
		g.Close()
		c.Close()
		t.Fatalf("failed to run migrations: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &SegmentedStorage{
		RuntimeDB:     r,
		TelemetryDB:   tel,
		GovernanceDB:  g,
		CognitionDB:   c,
		telemetryChan: make(chan TelemetryMetric, 100),
		ctx:           ctx,
		cancel:        cancel,
	}
}

func TestSovereignArbitrationLayer(t *testing.T) {
	storage := initTestSegmentedStorage(t)
	defer storage.Close()

	sal := NewSovereignArbitrationLayer(storage)

	// Intent 1: Coordination domain, workload "w1"
	intent1 := CognitiveIntent{
		ID:         "int-1",
		Domain:     DomainCoordination,
		WorkloadID: "w1",
		Action:     "deploy",
		Payload:    "spec1",
		Timestamp:  time.Now().Add(-5 * time.Second),
	}

	allowed, msg := sal.Arbitrate(intent1)
	if !allowed {
		t.Errorf("expected intent1 to be allowed, got: %s", msg)
	}

	// Intent 2: Governance domain (higher priority, 1 < 3), workload "w1"
	intent2 := CognitiveIntent{
		ID:         "int-2",
		Domain:     DomainGovernance,
		WorkloadID: "w1",
		Action:     "terminate",
		Payload:    "spec2",
		Timestamp:  time.Now(),
	}

	allowed, msg = sal.Arbitrate(intent2)
	if !allowed {
		t.Errorf("expected intent2 (governance) to override coordination, got rejected: %s", msg)
	}

	// Verify override in governance.db audit logs
	var count int
	err := storage.GovernanceDB.QueryRow("SELECT COUNT(*) FROM audit_logs WHERE operator = 'SAL_OVERRIDE' AND resource = 'w1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query audit logs: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 audit log entry for SAL_OVERRIDE, got %d", count)
	}

	// Intent 3: Coordination domain (lower priority than governance), workload "w1"
	intent3 := CognitiveIntent{
		ID:         "int-3",
		Domain:     DomainCoordination,
		WorkloadID: "w1",
		Action:     "deploy",
		Payload:    "spec3",
		Timestamp:  time.Now(),
	}

	allowed, msg = sal.Arbitrate(intent3)
	if allowed {
		t.Errorf("expected intent3 to be blocked by active higher-priority governance intent, but it was allowed")
	}

	// Verify rejection in governance.db audit logs
	err = storage.GovernanceDB.QueryRow("SELECT COUNT(*) FROM audit_logs WHERE operator = 'SAL_REJECT' AND resource = 'w1'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query audit logs: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 audit log entry for SAL_REJECT, got %d", count)
	}
}

func TestDeterministicEventCoordinationEngine(t *testing.T) {
	dece := &DeterministicEventCoordinationEngine{
		nodeID:      "test-node",
		epoch:       1,
		lamport:     0,
		subscribers: make(map[string]func(DECEEvent)),
		delayWindow: 10 * time.Millisecond, // short delay window for testing
	}

	// Test Lamport clock increments
	if val := dece.IncrementLamport(); val != 1 {
		t.Errorf("expected lamport to be 1, got %d", val)
	}

	if val := dece.UpdateLamport(5); val != 6 {
		t.Errorf("expected lamport to be 6 after update with 5, got %d", val)
	}

	// Set up events to sort
	// Sorted order should be:
	// 1. Epoch 1, Lamport 10, NodeID "node-A"
	// 2. Epoch 1, Lamport 10, NodeID "node-B"
	// 3. Epoch 1, Lamport 15, NodeID "node-A"
	// 4. Epoch 2, Lamport 5,  NodeID "node-A"
	pastTime := time.Now().Add(-1 * time.Second) // well outside delayWindow

	evt1 := DECEEvent{ID: "e1", Epoch: 1, Lamport: 15, NodeID: "node-A", Subject: "test", Timestamp: pastTime}
	evt2 := DECEEvent{ID: "e2", Epoch: 2, Lamport: 5, NodeID: "node-A", Subject: "test", Timestamp: pastTime}
	evt3 := DECEEvent{ID: "e3", Epoch: 1, Lamport: 10, NodeID: "node-B", Subject: "test", Timestamp: pastTime}
	evt4 := DECEEvent{ID: "e4", Epoch: 1, Lamport: 10, NodeID: "node-A", Subject: "test", Timestamp: pastTime}

	// Insert in non-sorted order
	dece.delayQueue = []DECEEvent{evt1, evt2, evt3, evt4}

	var delivered []string
	dece.subscribers["test"] = func(e DECEEvent) {
		delivered = append(delivered, e.ID)
	}

	dece.processQueue()

	if len(delivered) != 4 {
		t.Fatalf("expected 4 delivered events, got %d", len(delivered))
	}

	expectedOrder := []string{"e4", "e3", "e1", "e2"}
	for i, id := range expectedOrder {
		if delivered[i] != id {
			t.Errorf("at index %d, expected %s, got %s", i, id, delivered[i])
		}
	}
}

func TestCognitiveConvergenceFramework(t *testing.T) {
	storage := initTestSegmentedStorage(t)
	defer storage.Close()

	ccf := NewCognitiveConvergenceFramework(storage)
	ccf.windowDuration = 1 * time.Minute
	ccf.maxOscillations = 3

	// First transition: OK
	allowed, cooldown := ccf.RegisterStateTransition("w1", "running")
	if !allowed || cooldown != 0 {
		t.Errorf("expected first transition to be allowed with no cooldown")
	}

	// Second transition: OK
	allowed, cooldown = ccf.RegisterStateTransition("w1", "running")
	if !allowed || cooldown != 0 {
		t.Errorf("expected second transition to be allowed with no cooldown")
	}

	// Third transition: Oscillation loop triggered, cooldown applied!
	allowed, cooldown = ccf.RegisterStateTransition("w1", "running")
	if allowed || cooldown == 0 {
		t.Errorf("expected third transition to trigger damping and return disallowed")
	}

	// Verify damping recovery log entry in database
	var count int
	err := storage.CognitionDB.QueryRow("SELECT COUNT(*) FROM recovery_log WHERE workload_id = 'w1' AND action = 'OSCILLATION_DAMPING'").Scan(&count)
	if err != nil {
		t.Fatalf("failed to query recovery logs: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 recovery log entry, got %d", count)
	}
}

func TestAutonomousLoopIntegritySystem(t *testing.T) {
	alis := NewAutonomousLoopIntegritySystem()

	// Register two loops
	alis.RegisterLoop("loop-critical", 10*time.Millisecond)
	alis.RegisterLoop("loop-nonessential", 10*time.Millisecond)

	// Under normal conditions, both should run
	if !alis.CanExecute("loop-nonessential") {
		t.Errorf("expected normal execute allowed")
	}

	// Simulate loop-critical running taking 8ms out of a 10ms interval (0.8 density)
	alis.RecordExecution("loop-critical", 8*time.Millisecond)

	// Since total density (0.8) > 0.75, non-essential loop "mesh_sync" or "drift_regression" should be throttled
	alis.RegisterLoop("mesh_sync", 10*time.Millisecond)
	if alis.CanExecute("mesh_sync") {
		t.Errorf("expected non-essential loop 'mesh_sync' to be suppressed when density exceeds 0.75")
	}
}

func TestInfrastructureTemporalStabilityEngine(t *testing.T) {
	storage := initTestSegmentedStorage(t)
	defer storage.Close()

	itse := NewInfrastructureTemporalStabilityEngine(storage)

	// Inject 30 telemetry points
	// Let's create memory growth trend: mem increases by 2MB every tick (to create a large positive slope)
	for i := 0; i < 30; i++ {
		mem := int64(100 * 1024 * 1024 + i * 2 * 1024 * 1024)
		_, err := storage.TelemetryDB.Exec("INSERT INTO metrics (cpu, memory, temp) VALUES (?, ?, ?)", 25.0, mem, 45.0)
		if err != nil {
			t.Fatalf("failed to insert metric: %v", err)
		}
	}

	// Calculate drift. Memory slope is 2MB/s, normalized memory caps at 1.0. CPU variance is 0.
	// Drift should be 0.6 * 1.0 + 0.4 * 0.0 = 0.6.
	drift := itse.CalculateBehavioralDrift()
	if drift < 0.5 || drift > 0.7 {
		t.Errorf("expected drift to be around 0.6, got %.3f", drift)
	}

	// Let's inject a very high cpu variance to force drift > 0.85 and trigger compaction
	// Clear the database
	_, _ = storage.TelemetryDB.Exec("DELETE FROM metrics")

	for i := 0; i < 30; i++ {
		mem := int64(100 * 1024 * 1024 + i * 5 * 1024 * 1024) // high memory slope (5MB/s)
		cpu := 0.0
		if i%2 == 0 {
			cpu = 80.0
		} else {
			cpu = 5.0
		}
		_, err := storage.TelemetryDB.Exec("INSERT INTO metrics (cpu, memory, temp) VALUES (?, ?, ?)", cpu, mem, 45.0)
		if err != nil {
			t.Fatalf("failed to insert metric: %v", err)
		}
	}

	drift = itse.CalculateBehavioralDrift()
	if drift <= 0.85 {
		t.Errorf("expected drift to exceed 0.85, got %.3f", drift)
	}
}

func TestBoundedGraphTraversalEngine(t *testing.T) {
	storage := initTestSegmentedStorage(t)
	defer storage.Close()

	var err error

	// Insert nodes and edges to form a causal lineage chain
	// Intent -> Decision -> Action -> Effect -> Recovery
	// We will query lineage of workload "w1"
	if _, err = storage.CognitionDB.Exec("INSERT INTO graph_nodes (id, type, detail) VALUES ('node-1', 'Intent', 'spec-w1')"); err != nil {
		t.Logf("DEBUG: insert node-1 failed: %v", err)
	}
	if _, err = storage.CognitionDB.Exec("INSERT INTO graph_nodes (id, type, detail) VALUES ('node-2', 'Decision', 'schedule-dec')"); err != nil {
		t.Logf("DEBUG: insert node-2 failed: %v", err)
	}
	if _, err = storage.CognitionDB.Exec("INSERT INTO graph_nodes (id, type, detail) VALUES ('node-3', 'Action', 'deploy-act')"); err != nil {
		t.Logf("DEBUG: insert node-3 failed: %v", err)
	}
	if _, err = storage.CognitionDB.Exec("INSERT INTO graph_nodes (id, type, detail) VALUES ('node-4', 'Effect', 'running-eff')"); err != nil {
		t.Logf("DEBUG: insert node-4 failed: %v", err)
	}
	if _, err = storage.CognitionDB.Exec("INSERT INTO graph_nodes (id, type, detail) VALUES ('node-5', 'Recovery', 'crash-rec')"); err != nil {
		t.Logf("DEBUG: insert node-5 failed: %v", err)
	}

	if _, err = storage.CognitionDB.Exec("INSERT INTO graph_edges (from_node, to_node, relation_type) VALUES ('node-1', 'node-2', 'triggers')"); err != nil {
		t.Logf("DEBUG: insert edge 1-2 failed: %v", err)
	}
	if _, err = storage.CognitionDB.Exec("INSERT INTO graph_edges (from_node, to_node, relation_type) VALUES ('node-2', 'node-3', 'executes')"); err != nil {
		t.Logf("DEBUG: insert edge 2-3 failed: %v", err)
	}
	if _, err = storage.CognitionDB.Exec("INSERT INTO graph_edges (from_node, to_node, relation_type) VALUES ('node-3', 'node-4', 'produces')"); err != nil {
		t.Logf("DEBUG: insert edge 3-4 failed: %v", err)
	}
	if _, err = storage.CognitionDB.Exec("INSERT INTO graph_edges (from_node, to_node, relation_type) VALUES ('node-4', 'node-5', 'remediates')"); err != nil {
		t.Logf("DEBUG: insert edge 4-5 failed: %v", err)
	}

	bgte := NewBoundedGraphTraversalEngine(storage)

	nodes, edges, err := bgte.BoundedCausalLineage("w1")
	if err != nil {
		t.Fatalf("failed to query causal lineage: %v", err)
	}

	// Depth limit is 3. Starting at "node-1":
	// Hop 1: "node-2"
	// Hop 2: "node-3"
	// Hop 3: "node-4"
	// "node-5" should not be included (since depth 4 is excluded)
	expectedNodeCount := 4 // node-1, node-2, node-3, node-4
	if len(nodes) != expectedNodeCount {
		t.Errorf("expected %d nodes in lineage graph, got %d. Nodes: %v", expectedNodeCount, len(nodes), nodes)
	}

	expectedEdgeCount := 3 // triggers, executes, produces
	if len(edges) != expectedEdgeCount {
		t.Errorf("expected %d edges in lineage graph, got %d. Edges: %v", expectedEdgeCount, len(edges), edges)
	}
}

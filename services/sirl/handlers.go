package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

// handlePOSTWorkload schedules/deploys workloads
func (d *Daemon) handlePOSTWorkload(w http.ResponseWriter, r *http.Request) {
	var spec WorkloadSpec
	if err := json.NewDecoder(r.Body).Decode(&spec); err != nil {
		jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if spec.ID == "" || spec.Name == "" || spec.Image == "" {
		jsonError(w, "workload_id, name, and image are required fields", http.StatusBadRequest)
		return
	}

	// 1. Cognitive Convergence oscillation check
	allowed, cooldown := d.ccf.RegisterStateTransition(spec.ID, "running")
	if !allowed {
		jsonError(w, fmt.Sprintf("request blocked by Cognitive Convergence Framework. Active oscillation damping in progress. Cooldown: %v", cooldown), http.StatusTooManyRequests)
		return
	}

	// 2. Sovereign Arbitration precedence check
	intent := CognitiveIntent{
		ID:         fmt.Sprintf("int-%d", time.Now().UnixNano()),
		Domain:     DomainCoordination, // Scheduling requests fall in Coordination Domain
		WorkloadID: spec.ID,
		Action:     "deploy",
		Payload:    spec,
		Timestamp:  time.Now(),
	}
	authorized, reason := d.sal.Arbitrate(intent)
	if !authorized {
		// Clean ccf entry since it was rejected
		d.ccf.Clear(spec.ID)
		jsonError(w, fmt.Sprintf("request blocked by Sovereign Arbitration Layer: %s", reason), http.StatusConflict)
		return
	}

	cID, err := d.ScheduleWorkload(r.Context(), spec)
	if err != nil {
		// Clean active intents and ccf on failure
		d.sal.Clear(spec.ID)
		d.ccf.Clear(spec.ID)
		jsonError(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jsonOK(w, map[string]interface{}{
		"workload_id": spec.ID,
		"name":        spec.Name,
		"status":      "running",
		"container":   cID,
	})
}

// handleGETRecoveryStatus queries historical remediation logs from cognition.db
func (d *Daemon) handleGETRecoveryStatus(w http.ResponseWriter, r *http.Request) {
	workloadID := chi.URLParam(r, "id")
	if workloadID == "" {
		jsonError(w, "id param is required", http.StatusBadRequest)
		return
	}

	rows, err := d.storage.CognitionDB.QueryContext(r.Context(), `
		SELECT id, action, exit_code, outcome, timestamp FROM recovery_log
		WHERE workload_id = ? ORDER BY timestamp DESC LIMIT 50
	`, workloadID)
	if err != nil {
		jsonError(w, "db query error", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type Entry struct {
		ID        string    `json:"id"`
		Action    string    `json:"action"`
		ExitCode  int       `json:"exit_code"`
		Outcome   string    `json:"outcome"`
		Timestamp time.Time `json:"timestamp"`
	}

	var history []Entry
	for rows.Next() {
		var e Entry
		if err := rows.Scan(&e.ID, &e.Action, &e.ExitCode, &e.Outcome, &e.Timestamp); err == nil {
			history = append(history, e)
		}
	}

	jsonOK(w, map[string]interface{}{
		"workload_id": workloadID,
		"attempts":    history,
	})
}

// handleGETNodeState details host physical limits from telemetry.db and runtime.db
func (d *Daemon) handleGETNodeState(w http.ResponseWriter, r *http.Request) {
	totalMem, usedMem := readHostMemory()
	cpuPct := readHostCPU()
	temp := readHostTemperature()

	var activeWorkloads int
	_ = d.storage.RuntimeDB.QueryRow("SELECT COUNT(*) FROM workloads WHERE status = 'running'").Scan(&activeWorkloads)

	var peerCount int
	_ = d.storage.RuntimeDB.QueryRow("SELECT COUNT(*) FROM mesh_nodes WHERE status = 'online'").Scan(&peerCount)

	trust := 0.95
	_ = d.storage.RuntimeDB.QueryRow("SELECT trust_score FROM mesh_nodes WHERE node_id = ?", d.cfg.NodeID).Scan(&trust)

	jsonOK(w, map[string]interface{}{
		"node_id":          d.cfg.NodeID,
		"trust_score":      trust,
		"temperature_c":    temp,
		"active_workloads": activeWorkloads,
		"mesh_peers_count": peerCount,
		"resource_metrics": map[string]interface{}{
			"cpu_utilization_pct": cpuPct,
			"memory_used_bytes":   usedMem,
			"memory_total_bytes":  totalMem,
		},
	})
}

// handleGETIntentLineage returns the causal graph sequence for a workload
func (d *Daemon) handleGETIntentLineage(w http.ResponseWriter, r *http.Request) {
	workloadID := chi.URLParam(r, "id")
	if workloadID == "" {
		jsonError(w, "id param is required", http.StatusBadRequest)
		return
	}

	// Consume budget for graph queries (requires 12 credits)
	if allowed, err := d.budget.Consume("graph_traversal"); err != nil || !allowed {
		jsonError(w, err.Error(), http.StatusTooManyRequests)
		return
	}

	nodes, edges, err := d.bgte.BoundedCausalLineage(workloadID)
	if err != nil {
		jsonError(w, "failed to query causality lineage: "+err.Error(), http.StatusInternalServerError)
		return
	}

	jsonOK(w, map[string]interface{}{
		"workload_id": workloadID,
		"nodes":       nodes,
		"edges":       edges,
	})
}

// handleGETLoopStats queries ALIS registered loop statuses
func (d *Daemon) handleGETLoopStats(w http.ResponseWriter, r *http.Request) {
	d.alis.mu.Lock()
	stats := make(map[string]interface{})
	for k, v := range d.alis.registered {
		stats[k] = map[string]interface{}{
			"interval_ms":      v.Interval.Milliseconds(),
			"last_executed":    v.LastExecution.Format(time.RFC3339),
			"last_duration_ms": v.Duration.Milliseconds(),
		}
	}
	d.alis.mu.Unlock()
	jsonOK(w, stats)
}

// handlePOSTQuotaValidate checks and registers agent execution quotas
func (d *Daemon) handlePOSTQuotaValidate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		AgentID     string `json:"agent_id"`
		ParentDepth int    `json:"parent_depth"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		jsonError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// 1. Validate recursion depth boundary
	if err := d.sandbox.ValidateRecursionDepth(req.AgentID, req.ParentDepth); err != nil {
		jsonError(w, err.Error(), http.StatusForbidden)
		return
	}

	// 2. Validate action quota count
	if err := d.sandbox.ValidateActionQuota(req.AgentID); err != nil {
		jsonError(w, err.Error(), http.StatusForbidden)
		return
	}

	jsonOK(w, map[string]string{
		"agent_id": req.AgentID,
		"status":   "authorized",
	})
}

func jsonError(w http.ResponseWriter, msg string, code int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func jsonOK(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(data)
}

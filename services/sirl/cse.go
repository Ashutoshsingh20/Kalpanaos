package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"time"
)

type NodeSuitability struct {
	NodeID       string  `json:"node_id"`
	TotalScore   float64 `json:"total_score"`
	CPUScore     float64 `json:"cpu_score"`
	MemoryScore  float64 `json:"memory_score"`
	ThermalScore float64 `json:"thermal_score"`
	TrustScore   float64 `json:"trust_score"`
	NSI          float64 `json:"nsi"`
}

// CalculateNSI computes the Node Stability Index for a node based on historical errors
func (d *Daemon) CalculateNSI(nodeID string) float64 {
	var crashes, thermals, violations int

	// Count recovery/crash entries from cognition.db
	err := d.storage.CognitionDB.QueryRow(`
		SELECT COUNT(*) FROM recovery_log 
		WHERE outcome = 'failed' AND timestamp > datetime('now', '-24 hours')
	`).Scan(&crashes)
	if err != nil {
		crashes = 0
	}

	// Count thermal events (temperature > 75C) from telemetry.db
	err = d.storage.TelemetryDB.QueryRow(`
		SELECT COUNT(*) FROM metrics 
		WHERE temp > 75.0 AND timestamp > datetime('now', '-24 hours')
	`).Scan(&thermals)
	if err != nil {
		thermals = 0
	}

	// Count policy audit violations from governance.db
	err = d.storage.GovernanceDB.QueryRow(`
		SELECT COUNT(*) FROM audit_logs 
		WHERE outcome = 'denied' AND timestamp > datetime('now', '-24 hours')
	`).Scan(&violations)
	if err != nil {
		violations = 0
	}

	// Simulated successful uptime hours in past 24 hours
	successfulHours := 24.0

	nsi := successfulHours / (float64(crashes + thermals + violations) + 1.0)
	// Normalize NSI between 0.0 and 1.0 (clamping max NSI to 1.0)
	if nsi > 1.0 {
		nsi = 1.0
	}
	return nsi
}

// EvaluateSuitability calculates the placement score S(N, W) for the local node
func (d *Daemon) EvaluateSuitability(ctx context.Context, req WorkloadSpec) NodeSuitability {
	// 1. Resource Availability Factor
	totalMem, usedMem := readHostMemory()
	if totalMem == 0 {
		totalMem = 4 * 1024 * 1024 * 1024 // Fallback 4GB
		usedMem = 2 * 1024 * 1024 * 1024  // Fallback 2GB
	}

	remainingMemBytes := int64(totalMem - usedMem)
	requestedMem := req.MemoryLimitBytes
	if requestedMem == 0 {
		requestedMem = 128 * 1024 * 1024 // Default 128MB request
	}

	memScore := 1.0 - (float64(requestedMem) / float64(remainingMemBytes))
	if memScore < 0 {
		memScore = 0.0
	}

	// CPU Score
	cpuScore := 0.8
	var avgCPU float64
	err := d.storage.TelemetryDB.QueryRow("SELECT COALESCE(AVG(cpu), 0) FROM metrics").Scan(&avgCPU)
	if err == nil && avgCPU > 0 {
		cpuScore = 1.0 - (avgCPU / 100.0)
	}

	// 2. Thermal Drift Profile
	thermalScore := 1.0
	var avgTemp float64
	err = d.storage.TelemetryDB.QueryRow("SELECT COALESCE(AVG(temp), 0) FROM metrics").Scan(&avgTemp)
	if err == nil && avgTemp > 45 {
		criticalTemp := 85.0
		nominalTemp := 45.0
		if avgTemp > criticalTemp {
			thermalScore = 0.0
		} else {
			thermalScore = 1.0 - ((avgTemp - nominalTemp) / (criticalTemp - nominalTemp))
		}
	}

	// 3. Trust Score
	trustScore := 0.95
	err = d.storage.RuntimeDB.QueryRow("SELECT trust_score FROM mesh_nodes WHERE node_id = ?", d.cfg.NodeID).Scan(&trustScore)
	if err != nil && err != sql.ErrNoRows {
		log.Printf("[ACSE] Error fetching trust score: %v", err)
	}

	// Compute Node Stability Index (NSI)
	nsi := d.CalculateNSI(d.cfg.NodeID)

	// Calculate weighted aggregate score (w_r = 0.4, w_t = 0.2, w_s = 0.4)
	resourceWeight := 0.4
	thermalWeight := 0.2
	trustWeight := 0.4

	rawScore := (resourceWeight * (0.5*cpuScore + 0.5*memScore)) + (thermalWeight * thermalScore) + (trustWeight * trustScore)

	// Final Suitability Score incorporates NSI multiplier
	totalScore := nsi * rawScore
	totalScore = math.Round(totalScore*1000) / 1000

	return NodeSuitability{
		NodeID:       d.cfg.NodeID,
		TotalScore:   totalScore,
		CPUScore:     cpuScore,
		MemoryScore:  memScore,
		ThermalScore: thermalScore,
		TrustScore:   trustScore,
		NSI:          nsi,
	}
}

// ScheduleWorkload routes workload deployment locally or uses mesh bidding
func (d *Daemon) ScheduleWorkload(ctx context.Context, spec WorkloadSpec) (string, error) {
	log.Printf("[ACSE] Evaluating scheduling for workload %s...", spec.Name)

	// 1. Consume Cognition Budget for Scheduling Action (requires 12 credits)
	if allowed, err := d.budget.Consume("graph_traversal"); err != nil || !allowed {
		return "", fmt.Errorf("cognition budget exceeded: %w", err)
	}

	// 2. Pre-flight simulation check with RGE local policies
	allowed, err := d.EvaluateLocalPolicies(ctx, spec)
	if err != nil || !allowed {
		return "", fmt.Errorf("policy check rejected workload %s: %v", spec.Name, err)
	}

	// 3. Run predictive allocation simulation (dry-run check)
	sim := d.SimulateDeployment(ctx, spec)
	if !sim.Feasible {
		return "", fmt.Errorf("scheduling failed: dry-run simulation indicates memory starvation")
	}

	// 4. Calculate local ACSE suitability score
	localEval := d.EvaluateSuitability(ctx, spec)
	log.Printf("[ACSE] Suitability score: %.3f (NSI: %.3f)", localEval.TotalScore, localEval.NSI)

	// Create Intent and Decision nodes in Intent Graph
	intentID := fmt.Sprintf("intent-%d", time.Now().UnixNano())
	decisionID := fmt.Sprintf("dec-%d", time.Now().UnixNano())
	actionID := fmt.Sprintf("act-%d", time.Now().UnixNano())

	// Threshold suitability for local scheduling (0.75)
	if localEval.TotalScore >= 0.75 {
		log.Printf("[ACSE] Local score optimal. Executing deployment locally...")
		d.ige.RecordCausality(intentID, decisionID, actionID, spec.ID, fmt.Sprintf("Score %.3f optimal", localEval.TotalScore))
		return d.DeployWorkload(ctx, spec)
	}

	// If local score is low, attempt mesh negotiation
	if d.nc != nil && d.nc.IsConnected() {
		log.Printf("[ACSE] Local suitability low (%.3f). Requesting bids from peers...", localEval.TotalScore)
		winningNode, err := d.NegotiateMeshPlacement(ctx, spec)
		if err == nil && winningNode != d.cfg.NodeID {
			log.Printf("[ACSE] Bid awarded to peer node: %s", winningNode)
			d.ige.RecordCausality(intentID, decisionID, actionID, spec.ID, fmt.Sprintf("Awarded to peer %s", winningNode))
			return d.triggerRemoteDeploy(ctx, winningNode, spec)
		}
	}

	// Fallback to local execution if bidding fails or disconnected
	log.Printf("[ACSE] Bidding unavailable or failed. Falling back to local host.")
	d.ige.RecordCausality(intentID, decisionID, actionID, spec.ID, "Fallback to local host")
	return d.DeployWorkload(ctx, spec)
}

func (d *Daemon) triggerRemoteDeploy(ctx context.Context, peerNode string, spec WorkloadSpec) (string, error) {
	subject := fmt.Sprintf("kalpana.sirl.domain.runtime.commands.deploy.%s", peerNode)
	data, _ := json.Marshal(spec)
	resp, err := d.nc.Request(subject, data, 10*time.Second)
	if err != nil {
		return "", fmt.Errorf("remote deployment request timed out: %w", err)
	}

	var result struct {
		ContainerID string `json:"container_id"`
		Error       string `json:"error"`
	}
	if err := json.Unmarshal(resp.Data, &result); err != nil {
		return "", fmt.Errorf("failed to decode peer deploy response: %w", err)
	}

	if result.Error != "" {
		return "", fmt.Errorf("peer node deploy failed: %s", result.Error)
	}

	// Store allocation tracking locally in runtime.db
	_, _ = d.storage.RuntimeDB.Exec(`
		INSERT OR REPLACE INTO workloads (id, name, image, status, assigned_node, cpu_shares, memory_limit, updated_at)
		VALUES (?, ?, ?, 'running', ?, ?, ?, CURRENT_TIMESTAMP)
	`, spec.ID, spec.Name, spec.Image, peerNode, spec.CPUShares, spec.MemoryLimitBytes)

	return result.ContainerID, nil
}

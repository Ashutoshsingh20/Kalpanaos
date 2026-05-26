package main

import (
	"context"
	"log"
)

type SimulationResult struct {
	Feasible         bool    `json:"feasible"`
	ProjectedCPU     float64 `json:"projected_cpu"`
	ProjectedMemory  int64   `json:"projected_memory"`
	ConfidenceScore  float64 `json:"confidence_score"`
	StarvationDanger bool    `json:"starvation_danger"`
}

// SimulateDeployment runs a pre-flight resource allocation simulation
func (d *Daemon) SimulateDeployment(ctx context.Context, spec WorkloadSpec) SimulationResult {
	// Consume budget for simulation action (requires 8 credits)
	if allowed, err := d.budget.Consume("simulation"); err != nil || !allowed {
		return SimulationResult{Feasible: false}
	}

	totalMem, usedMem := readHostMemory()
	var avgCPU float64
	_ = d.storage.TelemetryDB.QueryRow("SELECT COALESCE(AVG(cpu), 0) FROM metrics").Scan(&avgCPU)

	requestedMem := spec.MemoryLimitBytes
	if requestedMem == 0 {
		requestedMem = 128 * 1024 * 1024
	}

	projectedMemUsed := int64(usedMem) + requestedMem
	projectedCPUUsed := avgCPU + 5.0 // Nominal estimate

	feasible := true
	starvationDanger := false

	if projectedMemUsed > int64(totalMem) {
		feasible = false
	} else if float64(projectedMemUsed)/float64(totalMem) > 0.90 {
		starvationDanger = true
	}

	if projectedCPUUsed > 90.0 {
		starvationDanger = true
	}

	confidence := 0.95
	if starvationDanger {
		confidence = 0.65
	}

	return SimulationResult{
		Feasible:         feasible,
		ProjectedCPU:     projectedCPUUsed,
		ProjectedMemory:  projectedMemUsed,
		ConfidenceScore:  confidence,
		StarvationDanger: starvationDanger,
	}
}

// ForecastDrift executes linear regression on memory growth vectors from telemetry.db
func (d *Daemon) ForecastDrift() {
	rows, err := d.storage.TelemetryDB.Query("SELECT cpu, memory FROM metrics ORDER BY timestamp ASC")
	if err != nil {
		return
	}
	defer rows.Close()

	var memoryPoints []float64
	for rows.Next() {
		var cpu float64
		var mem int64
		if err := rows.Scan(&cpu, &mem); err == nil {
			memoryPoints = append(memoryPoints, float64(mem))
		}
	}

	n := len(memoryPoints)
	if n < 10 {
		return
	}

	// Least Squares Linear Regression
	var sumX, sumY, sumXX, sumXY float64
	for i := 0; i < n; i++ {
		x := float64(i)
		y := memoryPoints[i]
		sumX += x
		sumY += y
		sumXX += x * x
		sumXY += x * y
	}

	slope := (float64(n)*sumXY - sumX*sumY) / (float64(n)*sumXX - sumX*sumX)

	if slope > 1024*1024 { // Growth speed > 1MB per sample (approx 10s)
		log.Printf("[PISL] WARNING: Node memory drift trend is POSITIVE (slope: %.2f MB/sec). Leak suspected.", slope/(1024*1024))

		// Send warning alert via ESGL (Priority 3: Cognitive - summarized before transmission)
		alertPayload := map[string]interface{}{
			"node_id":     d.cfg.NodeID,
			"alert_type":  "memory_drift_leak",
			"slope":       slope,
			"description": "Rapid resource allocation trend detected by Predictive Simulation Layer.",
		}
		d.esgl.Dispatch(Event{
			Subject:  "kalpana.sirl.domain.predictive.forecast.warnings",
			Priority: PriorityCognitive,
			Payload:  alertPayload,
		})
	}
}

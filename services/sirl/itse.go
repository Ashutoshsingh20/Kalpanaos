package main

import (
	"log"
	"math"
	"sync"
	"time"
)

type InfrastructureTemporalStabilityEngine struct {
	mu            sync.Mutex
	storage       *SegmentedStorage
	stabilityTime time.Time
	driftHistory  []float64
}

func NewInfrastructureTemporalStabilityEngine(storage *SegmentedStorage) *InfrastructureTemporalStabilityEngine {
	return &InfrastructureTemporalStabilityEngine{
		storage:       storage,
		stabilityTime: time.Now(),
	}
}

// CalculateBehavioralDrift computes drift metrics based on telemetry history.
// Returns drift score between 0.0 and 1.0.
func (itse *InfrastructureTemporalStabilityEngine) CalculateBehavioralDrift() float64 {
	itse.mu.Lock()
	defer itse.mu.Unlock()

	// 1. Memory and CPU trends from telemetry.db
	rows, err := itse.storage.TelemetryDB.Query("SELECT cpu, memory FROM metrics ORDER BY timestamp DESC LIMIT 30")
	if err != nil {
		return 0.0
	}
	defer rows.Close()

	var cpuValues []float64
	var memValues []float64
	for rows.Next() {
		var cpu float64
		var mem int64
		if err := rows.Scan(&cpu, &mem); err == nil {
			cpuValues = append(cpuValues, cpu)
			memValues = append(memValues, float64(mem))
		}
	}

	n := len(cpuValues)
	if n < 5 {
		return 0.0
	}

	// Compute memory growth slope (Regression)
	var sumX, sumY, sumXX, sumXY float64
	for i := 0; i < n; i++ {
		x := float64(i)
		y := memValues[i]
		sumX += x
		sumY += y
		sumXX += x * x
		sumXY += x * y
	}
	memSlope := (float64(n)*sumXY - sumX*sumY) / (float64(n)*sumXX - sumX*sumX)

	// Compute CPU variance
	var cpuMean float64
	for _, c := range cpuValues {
		cpuMean += c
	}
	cpuMean /= float64(n)

	var cpuVar float64
	for _, c := range cpuValues {
		cpuVar += (c - cpuMean) * (c - cpuMean)
	}
	cpuVar /= float64(n)

	// Normalize metrics: Slope of memory (capped) + variance of CPU (capped)
	// Target drift score between 0.0 and 1.0
	normalizedMem := math.Min(1.0, math.Abs(memSlope)/(1024*1024)) // Capped at 1MB/sec
	normalizedCPU := math.Min(1.0, cpuVar/400.0)                  // Capped at variance of 400

	drift := 0.6*normalizedMem + 0.4*normalizedCPU
	itse.driftHistory = append(itse.driftHistory, drift)
	if len(itse.driftHistory) > 100 {
		itse.driftHistory = itse.driftHistory[1:]
	}

	log.Printf("[ITSE] Node Behavioral Drift calculated: %.3f (Mem Slope: %.2f KB/s, CPU Variance: %.2f)",
		drift, memSlope/1024.0, cpuVar)

	// If drift exceeds stability bounds, trigger corrective action
	if drift > 0.85 {
		log.Printf("[ITSE] WARNING: Node behavioral drift (%.3f) exceeds equilibrium boundary! Forcing topology compaction.", drift)
		// Perform SQLite checkpointing to clean WAL and compress memory
		_, _ = itse.storage.TelemetryDB.Exec("PRAGMA wal_checkpoint(TRUNCATE);")
		_, _ = itse.storage.RuntimeDB.Exec("PRAGMA wal_checkpoint(TRUNCATE);")
		_, _ = itse.storage.CognitionDB.Exec("PRAGMA wal_checkpoint(TRUNCATE);")
	}

	return drift
}

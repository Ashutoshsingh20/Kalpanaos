package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type CognitiveConvergenceFramework struct {
	mu              sync.RWMutex
	stateChanges    map[string][]time.Time // WorkloadID -> change timestamps
	dampingEnd      map[string]time.Time   // WorkloadID -> damped till time
	storage         *SegmentedStorage
	windowDuration  time.Duration
	maxOscillations int
}

func NewCognitiveConvergenceFramework(storage *SegmentedStorage) *CognitiveConvergenceFramework {
	return &CognitiveConvergenceFramework{
		stateChanges:    make(map[string][]time.Time),
		dampingEnd:      make(map[string]time.Time),
		storage:         storage,
		windowDuration:  10 * time.Minute,
		maxOscillations: 3,
	}
}

// RegisterStateTransition records state transition and applies cognitive damping if oscillating.
// Returns (allowed, cooldownRemaining).
func (ccf *CognitiveConvergenceFramework) RegisterStateTransition(workloadID, targetState string) (bool, time.Duration) {
	ccf.mu.Lock()
	defer ccf.mu.Unlock()

	// Check if already damped
	if until, damped := ccf.dampingEnd[workloadID]; damped && time.Now().Before(until) {
		remaining := until.Sub(time.Now())
		log.Printf("[CCF] Warning: State transition for %s blocked. Node under active oscillation damping. Cooldown remaining: %v", workloadID, remaining)
		return false, remaining
	}

	now := time.Now()
	// Filter timestamps inside window
	var freshChanges []time.Time
	for _, t := range ccf.stateChanges[workloadID] {
		if now.Sub(t) < ccf.windowDuration {
			freshChanges = append(freshChanges, t)
		}
	}
	freshChanges = append(freshChanges, now)
	ccf.stateChanges[workloadID] = freshChanges

	count := len(freshChanges)
	if count >= ccf.maxOscillations {
		// Oscillation loop detected! Apply exponential damping cooldown
		multiplier := count - ccf.maxOscillations + 1
		cooldown := time.Duration(1<<multiplier) * 1 * time.Minute // 2^n minutes
		if cooldown > 2*time.Hour {
			cooldown = 2 * time.Hour // Cap at 2 hours edge survivability limit
		}
		ccf.dampingEnd[workloadID] = now.Add(cooldown)
		
		log.Printf("[CCF] CRITICAL: Oscillation loop detected on workload %s (%d transitions in %v). Damping applied for %v.",
			workloadID, count, ccf.windowDuration, cooldown)
		
		// Log oscillation warning to cognition.db
		_, _ = ccf.storage.CognitionDB.Exec(`
			INSERT INTO recovery_log (id, workload_id, action, exit_code, outcome)
			VALUES (?, ?, 'OSCILLATION_DAMPING', ?, 'damped')
		`, fmt.Sprintf("ccf-%d", now.UnixNano()), workloadID, count)

		return false, cooldown
	}

	return true, 0
}

func (ccf *CognitiveConvergenceFramework) Clear(workloadID string) {
	ccf.mu.Lock()
	defer ccf.mu.Unlock()
	delete(ccf.stateChanges, workloadID)
	delete(ccf.dampingEnd, workloadID)
}

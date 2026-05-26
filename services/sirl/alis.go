package main

import (
	"log"
	"sync"
	"time"
)

type LoopInfo struct {
	Name          string        `json:"name"`
	Interval      time.Duration `json:"interval"`
	LastExecution time.Time     `json:"last_execution"`
	Duration      time.Duration `json:"duration"`
}

type AutonomousLoopIntegritySystem struct {
	mu           sync.Mutex
	registered   map[string]LoopInfo
	cooldownEnds map[string]time.Time
}

func NewAutonomousLoopIntegritySystem() *AutonomousLoopIntegritySystem {
	return &AutonomousLoopIntegritySystem{
		registered:   make(map[string]LoopInfo),
		cooldownEnds: make(map[string]time.Time),
	}
}

func (a *AutonomousLoopIntegritySystem) RegisterLoop(name string, interval time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.registered[name] = LoopInfo{
		Name:     name,
		Interval: interval,
	}
}

// CanExecute checks if loop execution is allowed, or if loop entropy is too high forcing a cooldown
func (a *AutonomousLoopIntegritySystem) CanExecute(name string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if until, ok := a.cooldownEnds[name]; ok && time.Now().Before(until) {
		return false // Under active suppression
	}

	// Calculate loop execution density (Loop Entropy)
	totalDensity := 0.0
	for _, l := range a.registered {
		if l.Interval > 0 {
			density := float64(l.Duration) / float64(l.Interval)
			totalDensity += density
		}
	}

	// Bounded feedback loop suppression: if total density > 0.75, throttle non-essential loops
	if totalDensity > 0.75 {
		if name == "drift_regression" || name == "compaction" || name == "mesh_sync" {
			cooldown := 15 * time.Second
			a.cooldownEnds[name] = time.Now().Add(cooldown)
			log.Printf("[ALIS] Loop execution density (%.3f) exceeds bounds! Suppressing non-essential loop '%s' for %v",
				totalDensity, name, cooldown)
			return false
		}
	}

	return true
}

func (a *AutonomousLoopIntegritySystem) RecordExecution(name string, duration time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if info, exists := a.registered[name]; exists {
		info.LastExecution = time.Now()
		info.Duration = duration
		a.registered[name] = info
	}
}

package main

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
)

// StartControlLoops starts the background workers for the cognitive runtime
func (d *Daemon) StartControlLoops(ctx context.Context) {
	log.Printf("[SIRL] Initializing autonomous background control loops (HCD-aligned with ALIS)...")

	// Register loops with ALIS
	d.alis.RegisterLoop("telemetry_gossip", 5*time.Second)
	d.alis.RegisterLoop("reconciliation", 15*time.Second)
	d.alis.RegisterLoop("drift_regression", 30*time.Second)

	// 1. Telemetry and Gossip Loop (Every 5 seconds)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		gossipCounter := 0
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if d.alis.CanExecute("telemetry_gossip") {
					start := time.Now()
					d.GatherHostTelemetry()
					gossipCounter++
					if gossipCounter >= 2 { // Every 10s
						d.GossipCapabilities()
						gossipCounter = 0
					}
					d.alis.RecordExecution("telemetry_gossip", time.Since(start))
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// 2. Reconciliation & Self-Healing Supervision Loop (Every 15 seconds)
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if d.alis.CanExecute("reconciliation") {
					start := time.Now()
					d.ReconcileWorkloadStates(ctx)
					d.alis.RecordExecution("reconciliation", time.Since(start))
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// 3. Predictive Infrastructure Drift & Compaction Loop (Every 30 seconds)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		compactionCounter := 0
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if d.alis.CanExecute("drift_regression") {
					start := time.Now()
					d.ForecastDrift()
					
					// Compute temporal drift and publish SRDM metrics
					drift := d.itse.CalculateBehavioralDrift()
					
					var maxOsc int
					d.ccf.mu.RLock()
					for _, times := range d.ccf.stateChanges {
						if len(times) > maxOsc {
							maxOsc = len(times)
						}
					}
					d.ccf.mu.RUnlock()
					
					d.srdm.RecordStabilityMetrics(drift, float64(maxOsc))

					compactionCounter++
					if compactionCounter >= 20 { // Every 10 minutes
						log.Printf("[SIRL] Initiating cache and graph compaction checks...")
						d.alis.RegisterLoop("compaction", 10*time.Minute)
						if d.alis.CanExecute("compaction") {
							compactionStart := time.Now()
							d.ige.PurgeOldLineage()
							d.alis.RecordExecution("compaction", time.Since(compactionStart))
						}
						compactionCounter = 0
					}
					d.alis.RecordExecution("drift_regression", time.Since(start))
				}
			case <-ctx.Done():
				return
			}
		}
	}()
}

// ReconcileWorkloadStates reconciles desired workloads in runtime.db with actual running containers
func (d *Daemon) ReconcileWorkloadStates(ctx context.Context) {
	// Query desired workloads from runtime.db
	rows, err := d.storage.RuntimeDB.Query("SELECT id, name, status FROM workloads WHERE status = 'running' AND assigned_node = ?", d.cfg.NodeID)
	if err != nil {
		log.Printf("[SIRL] Reconcile DB query error: %v", err)
		return
	}
	defer rows.Close()

	// Gather actual running containers from Docker daemon
	containers, err := d.dockerCli.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		log.Printf("[SIRL] Reconcile Docker list error: %v", err)
		return
	}

	actualContainers := make(map[string]string)
	for _, c := range containers {
		for _, name := range c.Names {
			actualContainers[strings.TrimPrefix(name, "/")] = c.State
		}
	}

	for rows.Next() {
		var id, name, status string
		if err := rows.Scan(&id, &name, &status); err != nil {
			continue
		}

		containerName := "kalpana-" + name
		actualState, exists := actualContainers[containerName]

		if !exists {
			log.Printf("[SIRL] Reconciliation Alert: Container %s missing! Initiating recovery stabilization...", containerName)
			go func(wID string) {
				_ = d.HandleWorkloadFailure(context.Background(), wID, -1)
			}(id)
		} else if actualState != "running" {
			log.Printf("[SIRL] Reconciliation Alert: Container %s in state %s. Initiating recovery stabilization...", containerName, actualState)
			go func(wID string) {
				_ = d.HandleWorkloadFailure(context.Background(), wID, 1)
			}(id)
		}
	}
}

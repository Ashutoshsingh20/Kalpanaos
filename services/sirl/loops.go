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
	log.Printf("[SIRL] Initializing autonomous background control loops (HCD-aligned)...")

	// 1. Telemetry and Gossip Loop (Every 5 seconds)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		gossipCounter := 0
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				d.GatherHostTelemetry()
				gossipCounter++
				if gossipCounter >= 2 { // Every 10s
					d.GossipCapabilities()
					gossipCounter = 0
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
				d.ReconcileWorkloadStates(ctx)
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
				d.ForecastDrift()
				compactionCounter++
				if compactionCounter >= 20 { // Every 10 minutes
					log.Printf("[SIRL] Initiating cache and graph compaction checks...")
					d.ige.PurgeOldLineage()
					compactionCounter = 0
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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
)

type RecoveryLogEntry struct {
	ID         string    `json:"id"`
	WorkloadID string    `json:"workload_id"`
	Action     string    `json:"action"`
	ExitCode   int       `json:"exit_code"`
	Outcome    string    `json:"outcome"`
	Timestamp  time.Time `json:"timestamp"`
}

// HandleWorkloadFailure evaluates and transitions workload through the RSF state machine
func (d *Daemon) HandleWorkloadFailure(ctx context.Context, workloadID string, exitCode int) error {
	log.Printf("[RSF] Crash intercepted for workload %s (Exit Code: %d). Resolving recovery state...", workloadID, exitCode)

	// Consume budget for recovery analysis (requires 7 credits)
	if allowed, err := d.budget.Consume("recovery_analysis"); err != nil || !allowed {
		return fmt.Errorf("cognition budget exceeded: %w", err)
	}

	var wl WorkloadSpec
	var status string
	err := d.storage.RuntimeDB.QueryRow("SELECT id, name, image, status, cpu_shares, memory_limit FROM workloads WHERE id = ?", workloadID).Scan(
		&wl.ID, &wl.Name, &wl.Image, &status, &wl.CPUShares, &wl.MemoryLimitBytes,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("crashed workload %s not found in runtime database", workloadID)
		}
		return err
	}

	// Calculate recent failure history from cognition.db
	var failures int
	err = d.storage.CognitionDB.QueryRow(`
		SELECT COUNT(*) FROM recovery_log 
		WHERE workload_id = ? AND timestamp > datetime('now', '-10 minutes') AND outcome = 'failed'
	`, workloadID).Scan(&failures)
	if err != nil {
		failures = 0
	}

	entryID := fmt.Sprintf("rec-%d", time.Now().UnixNano())

	// State Machine Decision Tree
	switch {
	case failures < 2:
		// State: RECOVERING (Exponential Cooldown Restart)
		cooldown := time.Duration(1<<failures) * 10 * time.Second
		log.Printf("[RSF] State: RECOVERING. Applying exponential cooldown (%v) before restart...", cooldown)
		time.Sleep(cooldown)

		d.updateWorkloadStatus(workloadID, "recovering")
		err := d.RestartWorkloadContainer(ctx, wl.Name)
		if err != nil {
			d.logRecoveryAttempt(entryID, workloadID, "RESTART_CONTAINER", exitCode, "failed")
			return err
		}
		d.logRecoveryAttempt(entryID, workloadID, "RESTART_CONTAINER", exitCode, "success")
		d.updateWorkloadStatus(workloadID, "running")
		log.Printf("[RSF] Successfully restarted container %s", wl.Name)
		return nil

	case failures >= 2 && failures < 4:
		// State: DEGRADED (Fallback OCI Image Rollback & CPU Throttling)
		log.Printf("[RSF] State: DEGRADED. Failure count (%d) triggered OCI rollback and throttling...", failures)
		d.updateWorkloadStatus(workloadID, "degraded")

		prevImage, err := d.fetchLastStableImage(wl.Name, wl.Image)
		if err != nil || prevImage == "" {
			log.Printf("[RSF] No historical stable image found for rollback. Escalating state...")
			return d.quarantineWorkloadLocally(ctx, workloadID, exitCode, entryID)
		}

		log.Printf("[RSF] Rolling back workload %s to stable image: %s", wl.Name, prevImage)
		wl.Image = prevImage
		// Enforce throttling: clamp CPU shares to 10% (102 shares) to prevent host exhaustion
		wl.CPUShares = 102

		_ = d.TerminateWorkload(ctx, workloadID)
		_, err = d.DeployWorkload(ctx, wl)
		if err != nil {
			d.logRecoveryAttempt(entryID, workloadID, "ROLLBACK_OCI_IMAGE", exitCode, "failed")
			return d.quarantineWorkloadLocally(ctx, workloadID, exitCode, entryID)
		}

		d.logRecoveryAttempt(entryID, workloadID, "ROLLBACK_OCI_IMAGE", exitCode, "success")
		log.Printf("[RSF] Workload %s recovered in Degraded State (Throttled CPU).", wl.Name)
		return nil

	default:
		// State: QUARANTINED (Shutdown and Isolation)
		return d.quarantineWorkloadLocally(ctx, workloadID, exitCode, entryID)
	}
}

func (d *Daemon) quarantineWorkloadLocally(ctx context.Context, workloadID string, exitCode int, entryID string) error {
	log.Printf("[RSF] State: QUARANTINED. Shutting down container %s to protect node integrity.", workloadID)
	_ = d.TerminateWorkload(ctx, workloadID)

	d.updateWorkloadStatus(workloadID, "quarantined")
	d.logRecoveryAttempt(entryID, workloadID, "QUARANTINE", exitCode, "success")

	// Publish node warning alert via ESGL (Priority 1: Critical - dispatched immediately)
	alert := map[string]interface{}{
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
		"workload_id": workloadID,
		"status":      "quarantined",
		"reason":      "persistent failure loop storm blocked by RSF state machine",
	}
	d.esgl.Dispatch(Event{
		Subject:  "kalpana.sirl.domain.predictive.forecast.warnings",
		Priority: PriorityCritical,
		Payload:  alert,
	})

	return fmt.Errorf("workload persistently failed and has been quarantined")
}

func (d *Daemon) RestartWorkloadContainer(ctx context.Context, serviceName string) error {
	containerName := "kalpana-" + serviceName
	containers, err := d.dockerCli.ContainerList(ctx, container.ListOptions{All: true})
	if err != nil {
		return err
	}
	var cID string
	for _, c := range containers {
		for _, name := range c.Names {
			if strings.TrimPrefix(name, "/") == containerName {
				cID = c.ID
				break
			}
		}
	}
	if cID == "" {
		return fmt.Errorf("container not found")
	}
	timeout := 15
	return d.dockerCli.ContainerRestart(ctx, cID, container.StopOptions{Timeout: &timeout})
}

func (d *Daemon) updateWorkloadStatus(workloadID, status string) {
	_, err := d.storage.RuntimeDB.Exec("UPDATE workloads SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?", status, workloadID)
	if err != nil {
		log.Printf("[RSF] Failed to update status in DB: %v", err)
	}
}

func (d *Daemon) logRecoveryAttempt(id, workloadID, action string, exitCode int, outcome string) {
	_, err := d.storage.CognitionDB.Exec(`
		INSERT INTO recovery_log (id, workload_id, action, exit_code, outcome)
		VALUES (?, ?, ?, ?, ?)
	`, id, workloadID, action, exitCode, outcome)
	if err != nil {
		log.Printf("[RSF] Failed to log recovery attempt: %v", err)
	}
}

func (d *Daemon) fetchLastStableImage(serviceName, currentImage string) (string, error) {
	rows, err := d.storage.RuntimeDB.Query(`
		SELECT DISTINCT image FROM workloads 
		WHERE name = ? AND image != ? AND status = 'running'
		ORDER BY created_at DESC LIMIT 5
	`, serviceName, currentImage)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	if rows.Next() {
		var img string
		if err := rows.Scan(&img); err == nil {
			return img, nil
		}
	}

	return "", nil
}

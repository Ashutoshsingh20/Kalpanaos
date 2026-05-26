package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type SegmentedStorage struct {
	RuntimeDB    *sql.DB
	TelemetryDB  *sql.DB
	GovernanceDB *sql.DB
	CognitionDB  *sql.DB

	telemetryChan chan TelemetryMetric
	wg            sync.WaitGroup
	ctx           context.Context
	cancel        context.CancelFunc
}

type TelemetryMetric struct {
	CPUUsage float64
	MemUsed  uint64
	Temp     float64
}

func InitSegmentedStorage(dirPath string) (*SegmentedStorage, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create db directory: %w", err)
	}

	// 1. Establish DB Connections & Configurations
	runtimeDB, err := openAndConfigureDB(filepath.Join(dirPath, "runtime.db"), "NORMAL", "20480")
	if err != nil {
		return nil, fmt.Errorf("runtime.db failed: %w", err)
	}

	telemetryDB, err := openAndConfigureDB(filepath.Join(dirPath, "telemetry.db"), "OFF", "10240")
	if err != nil {
		runtimeDB.Close()
		return nil, fmt.Errorf("telemetry.db failed: %w", err)
	}

	governanceDB, err := openAndConfigureDB(filepath.Join(dirPath, "governance.db"), "FULL", "15360")
	if err != nil {
		runtimeDB.Close()
		telemetryDB.Close()
		return nil, fmt.Errorf("governance.db failed: %w", err)
	}

	cognitionDB, err := openAndConfigureDB(filepath.Join(dirPath, "cognition.db"), "NORMAL", "20480")
	if err != nil {
		runtimeDB.Close()
		telemetryDB.Close()
		governanceDB.Close()
		return nil, fmt.Errorf("cognition.db failed: %w", err)
	}

	// 2. Database-specific migrations
	if err := runMigrations(runtimeDB, telemetryDB, governanceDB, cognitionDB); err != nil {
		runtimeDB.Close()
		telemetryDB.Close()
		governanceDB.Close()
		cognitionDB.Close()
		return nil, fmt.Errorf("db migrations failed: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	store := &SegmentedStorage{
		RuntimeDB:     runtimeDB,
		TelemetryDB:   telemetryDB,
		GovernanceDB:  governanceDB,
		CognitionDB:   cognitionDB,
		telemetryChan: make(chan TelemetryMetric, 1000), // pre-allocated ring buffer
		ctx:           ctx,
		cancel:        cancel,
	}

	// Start async telemetry batch writer
	store.wg.Add(1)
	go store.startTelemetryWriter()

	return store, nil
}

func openAndConfigureDB(path, syncMode, cacheSize string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // Single writer for safety

	pragmas := []string{
		fmt.Sprintf("PRAGMA synchronous = %s;", syncMode),
		fmt.Sprintf("PRAGMA cache_size = -%s;", cacheSize), // Negative means Kibibytes (e.g. -20000 is ~20MB)
		"PRAGMA temp_store = MEMORY;",
		"PRAGMA journal_size_limit = 5242880;", // Capped WAL size at 5MB to save disk
	}

	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			db.Close()
			return nil, fmt.Errorf("pragma configuration %s failed: %w", p, err)
		}
	}

	return db, nil
}

func runMigrations(r, t, g, c *sql.DB) error {
	// runtime.db
	runtimeSchema := `
	CREATE TABLE IF NOT EXISTS workloads (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL,
		image TEXT NOT NULL,
		status TEXT NOT NULL, -- normal, recovering, degraded, quarantined
		assigned_node TEXT NOT NULL,
		cpu_shares INTEGER,
		memory_limit INTEGER,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS mesh_nodes (
		node_id TEXT PRIMARY KEY,
		trust_score REAL,
		cpu_cores INTEGER,
		memory_total INTEGER,
		status TEXT,
		last_seen TIMESTAMP
	);
	`
	if _, err := r.Exec(runtimeSchema); err != nil {
		return err
	}

	// telemetry.db
	telemetrySchema := `
	CREATE TABLE IF NOT EXISTS metrics (
		timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		cpu REAL,
		memory INTEGER,
		temp REAL
	);
	`
	if _, err := t.Exec(telemetrySchema); err != nil {
		return err
	}

	// governance.db
	govSchema := `
	CREATE TABLE IF NOT EXISTS audit_logs (
		id TEXT PRIMARY KEY,
		operator TEXT NOT NULL,
		action TEXT NOT NULL,
		resource TEXT NOT NULL,
		outcome TEXT NOT NULL,
		timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS agent_quotas (
		agent_id TEXT PRIMARY KEY,
		action_count INTEGER DEFAULT 0,
		recursion_depth INTEGER DEFAULT 0,
		last_reset TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	`
	if _, err := g.Exec(govSchema); err != nil {
		return err
	}

	// cognition.db
	cognitionSchema := `
	CREATE TABLE IF NOT EXISTS recovery_log (
		id TEXT PRIMARY KEY,
		workload_id TEXT,
		action TEXT,
		exit_code INTEGER,
		outcome TEXT,
		timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS graph_nodes (
		id TEXT PRIMARY KEY,
		type TEXT NOT NULL,
		detail TEXT NOT NULL,
		timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS graph_edges (
		from_node TEXT NOT NULL,
		to_node TEXT NOT NULL,
		relation_type TEXT NOT NULL,
		PRIMARY KEY (from_node, to_node, relation_type),
		FOREIGN KEY (from_node) REFERENCES graph_nodes(id),
		FOREIGN KEY (to_node) REFERENCES graph_nodes(id)
	);
	`
	_, err := c.Exec(cognitionSchema)
	return err
}

func (s *SegmentedStorage) WriteTelemetryAsync(cpu float64, mem uint64, temp float64) {
	select {
	case s.telemetryChan <- TelemetryMetric{CPUUsage: cpu, MemUsed: mem, Temp: temp}:
	default:
		// Drop metric if queue full to prevent blocking runtime loops
	}
}

func (s *SegmentedStorage) Close() {
	s.cancel()
	s.wg.Wait()

	s.RuntimeDB.Close()
	s.TelemetryDB.Close()
	s.GovernanceDB.Close()
	s.CognitionDB.Close()
}

func (s *SegmentedStorage) startTelemetryWriter() {
	defer s.wg.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	var batch []TelemetryMetric
	flush := func() {
		if len(batch) == 0 {
			return
		}

		tx, err := s.TelemetryDB.Begin()
		if err != nil {
			log.Printf("[Storage] Async telemetry tx begin failed: %v", err)
			return
		}

		stmt, err := tx.Prepare("INSERT INTO metrics (cpu, memory, temp) VALUES (?, ?, ?)")
		if err != nil {
			tx.Rollback()
			return
		}
		defer stmt.Close()

		for _, m := range batch {
			_, _ = stmt.Exec(m.CPUUsage, m.MemUsed, m.Temp)
		}

		if err := tx.Commit(); err != nil {
			log.Printf("[Storage] Async telemetry tx commit failed: %v", err)
		}

		// Enforce storage size limits: keep telemetry database to last 1 hour
		_, _ = s.TelemetryDB.Exec("DELETE FROM metrics WHERE timestamp < datetime('now', '-1 hour')")

		batch = batch[:0]
	}

	for {
		select {
		case metric := <-s.telemetryChan:
			batch = append(batch, metric)
			if len(batch) >= 50 {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-s.ctx.Done():
			flush()
			return
		}
	}
}

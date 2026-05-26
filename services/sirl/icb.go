package main

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
)

type CognitionBudgetingEngine struct {
	mu            sync.Mutex
	credits       float64
	maxCredits    float64
	lastReplenish time.Time
}

func NewCognitionBudgetingEngine() *CognitionBudgetingEngine {
	return &CognitionBudgetingEngine{
		credits:       100.0,
		maxCredits:    100.0,
		lastReplenish: time.Now(),
	}
}

// Consume check if node has enough credits for the cognitive operation
func (icb *CognitionBudgetingEngine) Consume(actionType string) (bool, error) {
	icb.mu.Lock()
	defer icb.mu.Unlock()

	// Replenish first: 2 credits per second since last check
	now := time.Now()
	seconds := now.Sub(icb.lastReplenish).Seconds()
	if seconds > 0 {
		icb.credits += seconds * 2.0
		if icb.credits > icb.maxCredits {
			icb.credits = icb.maxCredits
		}
		icb.lastReplenish = now
	}

	// Read host memory dynamically to apply credit caps
	total, used := readHostMemory()
	if total > 0 && float64(used)/float64(total) > 0.80 {
		// RAM > 80% used: clamp max credits to 30 to prevent system starvation
		icb.maxCredits = 30.0
		if icb.credits > icb.maxCredits {
			icb.credits = icb.maxCredits
		}
	} else {
		icb.maxCredits = 100.0
	}

	cost := 0.0
	switch actionType {
	case "embedding":
		cost = 5.0
	case "simulation":
		cost = 8.0
	case "graph_traversal":
		cost = 12.0
	case "recovery_analysis":
		cost = 7.0
	default:
		cost = 2.0
	}

	if icb.credits >= cost {
		icb.credits -= cost
		return true, nil
	}

	return false, fmt.Errorf("cognition budget exhausted: action %s requires %.1f credits, only %.1f available", actionType, cost, icb.credits)
}

type AutonomousGovernanceSandbox struct {
	db *sql.DB
}

func NewAutonomousGovernanceSandbox(db *sql.DB) *AutonomousGovernanceSandbox {
	return &AutonomousGovernanceSandbox{db: db}
}

// ValidateRecursionDepth intercepts child tasks to prevent infinite agent execution loops
func (ags *AutonomousGovernanceSandbox) ValidateRecursionDepth(agentID string, depth int) error {
	maxDepth := 3
	if depth > maxDepth {
		return fmt.Errorf("autonomy violation: agent %s exceeded maximum recursion depth of %d", agentID, maxDepth)
	}

	// Update active depth in DB
	_, err := ags.db.Exec(`
		INSERT OR REPLACE INTO agent_quotas (agent_id, recursion_depth, last_reset)
		VALUES (?, ?, CURRENT_TIMESTAMP)
	`, agentID, depth)
	return err
}

// ValidateActionQuota limits mutating actions (deployments, scale, restart, rollbacks) to 5 per hour per agent
func (ags *AutonomousGovernanceSandbox) ValidateActionQuota(agentID string) error {
	// Clean up old quotas if last reset was more than 1 hour ago
	_, _ = ags.db.Exec(`
		UPDATE agent_quotas SET action_count = 0, last_reset = CURRENT_TIMESTAMP
		WHERE last_reset < datetime('now', '-1 hour')
	`)

	var count int
	err := ags.db.QueryRow("SELECT COALESCE(action_count, 0) FROM agent_quotas WHERE agent_id = ?", agentID).Scan(&count)
	if err != nil && err != sql.ErrNoRows {
		return err
	}

	maxActions := 5
	if count >= maxActions {
		return fmt.Errorf("governance quota exceeded: agent %s executed %d actions in the past hour (max: %d)", agentID, count, maxActions)
	}

	// Increment action count
	_, err = ags.db.Exec(`
		INSERT INTO agent_quotas (agent_id, action_count, last_reset)
		VALUES (?, ?, CURRENT_TIMESTAMP)
		ON CONFLICT(agent_id) DO UPDATE SET
			action_count = action_count + 1,
			last_reset = CURRENT_TIMESTAMP
	`, agentID, count+1)

	return err
}

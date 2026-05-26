package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type IntentDomain int

const (
	DomainGovernance   IntentDomain = 1
	DomainRecovery     IntentDomain = 2
	DomainCoordination IntentDomain = 3
	DomainPredictive   IntentDomain = 4
)

type CognitiveIntent struct {
	ID         string       `json:"id"`
	Domain     IntentDomain `json:"domain"`
	WorkloadID string       `json:"workload_id"`
	Action     string       `json:"action"` // deploy, restart, terminate, migrate
	Payload    interface{}  `json:"payload"`
	Timestamp  time.Time    `json:"timestamp"`
}

type SovereignArbitrationLayer struct {
	mu            sync.RWMutex
	activeIntents map[string]CognitiveIntent // WorkloadID -> active intent
	storage       *SegmentedStorage
}

func NewSovereignArbitrationLayer(storage *SegmentedStorage) *SovereignArbitrationLayer {
	return &SovereignArbitrationLayer{
		activeIntents: make(map[string]CognitiveIntent),
		storage:       storage,
	}
}

// Arbitrate resolves overlaps and decides if the action is allowed
func (sal *SovereignArbitrationLayer) Arbitrate(intent CognitiveIntent) (bool, string) {
	sal.mu.Lock()
	defer sal.mu.Unlock()

	// Check if there is an active intent for this workload
	existing, found := sal.activeIntents[intent.WorkloadID]
	if !found {
		sal.activeIntents[intent.WorkloadID] = intent
		return true, "no conflicting intents, authorized"
	}

	// Precedence check: DomainGovernance > DomainRecovery > DomainCoordination > DomainPredictive
	// Lower values in IntentDomain have higher priority
	if intent.Domain < existing.Domain {
		log.Printf("[SAL] Overriding active intent %s (Domain %d) with higher-priority intent %s (Domain %d) for workload %s",
			existing.Action, existing.Domain, intent.Action, intent.Domain, intent.WorkloadID)
		sal.activeIntents[intent.WorkloadID] = intent
		
		// Log override in governance.db
		_, _ = sal.storage.GovernanceDB.Exec(`
			INSERT INTO audit_logs (id, operator, action, resource, outcome)
			VALUES (?, 'SAL_OVERRIDE', ?, ?, 'override_success')
		`, fmt.Sprintf("sal-%d", time.Now().UnixNano()), fmt.Sprintf("override:%s_with_%s", existing.Action, intent.Action), intent.WorkloadID)
		
		return true, fmt.Sprintf("authorized, overrides existing %s action", existing.Action)
	}

	// If same priority, oldest wins or tie-breaker
	if intent.Domain == existing.Domain {
		if intent.Timestamp.Before(existing.Timestamp) {
			sal.activeIntents[intent.WorkloadID] = intent
			return true, "authorized, older timestamp tie-breaker"
		}
		return false, "rejected, identical domain priority but newer timestamp"
	}

	// Log rejection in governance.db
	_, _ = sal.storage.GovernanceDB.Exec(`
		INSERT INTO audit_logs (id, operator, action, resource, outcome)
		VALUES (?, 'SAL_REJECT', ?, ?, 'arbitration_blocked')
	`, fmt.Sprintf("sal-%d", time.Now().UnixNano()), intent.Action, intent.WorkloadID)

	return false, fmt.Sprintf("rejected, blocked by active higher-priority intent %s (Domain %d)", existing.Action, existing.Domain)
}

func (sal *SovereignArbitrationLayer) Clear(workloadID string) {
	sal.mu.Lock()
	defer sal.mu.Unlock()
	delete(sal.activeIntents, workloadID)
}

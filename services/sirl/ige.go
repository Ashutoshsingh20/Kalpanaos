package main

import (
	"database/sql"
	"log"
	"time"
)

type IntentGraphEngine struct {
	db *sql.DB
}

type GraphNode struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"` // intent | decision | action | effect | recovery | memory
	Detail    string    `json:"detail"`
	Timestamp time.Time `json:"timestamp"`
}

type GraphEdge struct {
	FromNode     string `json:"from_node"`
	ToNode       string `json:"to_node"`
	RelationType string `json:"relation_type"` // CAUSE_OF | TRIGGERED_BY | ENFORCED_BY | MUTATED_TO
}

func NewIntentGraphEngine(db *sql.DB) *IntentGraphEngine {
	return &IntentGraphEngine{db: db}
}

func (ige *IntentGraphEngine) AddNode(id, nodeType, detail string) error {
	_, err := ige.db.Exec(`
		INSERT OR IGNORE INTO graph_nodes (id, type, detail)
		VALUES (?, ?, ?)
	`, id, nodeType, detail)
	return err
}

func (ige *IntentGraphEngine) AddEdge(from, to, relation string) error {
	_, err := ige.db.Exec(`
		INSERT OR IGNORE INTO graph_edges (from_node, to_node, relation_type)
		VALUES (?, ?, ?)
	`, from, to, relation)
	return err
}

// RecordCausality links a deployment or recovery timeline causally in the graph DB
func (ige *IntentGraphEngine) RecordCausality(intentID, decisionID, actionID, workloadID, details string) {
	// 1. Add Nodes
	_ = ige.AddNode(intentID, "intent", "Workload deploy requested for "+workloadID)
	_ = ige.AddNode(decisionID, "decision", "Placement decision for "+workloadID+": "+details)
	_ = ige.AddNode(actionID, "action", "Container deployment executed by RIB")

	// 2. Link Edges
	_ = ige.AddEdge(intentID, decisionID, "CAUSE_OF")
	_ = ige.AddEdge(decisionID, actionID, "CAUSE_OF")

	log.Printf("[IGE] Causality recorded for workload %s (Intent: %s -> Decision: %s -> Action: %s)", workloadID, intentID, decisionID, actionID)
}

// GetCausalLineage traverses downstream relations to return the causality lifecycle of a workload
func (ige *IntentGraphEngine) GetCausalLineage(workloadID string) ([]GraphNode, []GraphEdge, error) {
	// Look up nodes and edges associated with the workload from cognition.db
	// For simplicity and edge efficiency, we query nodes linked to the workload execution trace
	rows, err := ige.db.Query(`
		SELECT n.id, n.type, n.detail, n.timestamp FROM graph_nodes n
		WHERE n.detail LIKE ? OR n.id IN (
			SELECT from_node FROM graph_edges WHERE to_node IN (
				SELECT id FROM graph_nodes WHERE detail LIKE ?
			)
		)
		ORDER BY n.timestamp ASC
	`, "%"+workloadID+"%", "%"+workloadID+"%")
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var nodes []GraphNode
	nodeIDs := make(map[string]bool)
	for rows.Next() {
		var n GraphNode
		if err := rows.Scan(&n.ID, &n.Type, &n.Detail, &n.Timestamp); err == nil {
			nodes = append(nodes, n)
			nodeIDs[n.ID] = true
		}
	}

	var edges []GraphEdge
	if len(nodes) > 0 {
		edgeRows, err := ige.db.Query(`SELECT from_node, to_node, relation_type FROM graph_edges`)
		if err == nil {
			defer edgeRows.Close()
			for edgeRows.Next() {
				var e GraphEdge
				if err := edgeRows.Scan(&e.FromNode, &e.ToNode, &e.RelationType); err == nil {
					if nodeIDs[e.FromNode] || nodeIDs[e.ToNode] {
						edges = append(edges, e)
					}
				}
			}
		}
	}

	return nodes, edges, nil
}

// PurgeOldLineage cleans graph databases to avoid edge disk saturation
func (ige *IntentGraphEngine) PurgeOldLineage() {
	// Delete edges older than 72 hours
	_, _ = ige.db.Exec(`
		DELETE FROM graph_edges WHERE from_node IN (
			SELECT id FROM graph_nodes WHERE timestamp < datetime('now', '-72 hours')
		) OR to_node IN (
			SELECT id FROM graph_nodes WHERE timestamp < datetime('now', '-72 hours')
		)
	`)

	// Delete nodes older than 72 hours
	_, _ = ige.db.Exec("DELETE FROM graph_nodes WHERE timestamp < datetime('now', '-72 hours')")
}

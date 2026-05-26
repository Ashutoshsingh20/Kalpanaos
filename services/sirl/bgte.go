package main

import (
	"sync"
	"time"
)

type BoundedGraphTraversalEngine struct {
	mu      sync.Mutex
	storage *SegmentedStorage
	cache   map[string]BGTECacheEntry
}

type BGTECacheEntry struct {
	Nodes     []GraphNode
	Edges     []GraphEdge
	Timestamp time.Time
}

func NewBoundedGraphTraversalEngine(storage *SegmentedStorage) *BoundedGraphTraversalEngine {
	return &BoundedGraphTraversalEngine{
		storage: storage,
		cache:   make(map[string]BGTECacheEntry),
	}
}

// BoundedCausalLineage executes depth-limited Intent Graph traversals.
func (b *BoundedGraphTraversalEngine) BoundedCausalLineage(workloadID string) ([]GraphNode, []GraphEdge, error) {
	b.mu.Lock()
	// Check cache (stabilized traversals cached for 10 seconds)
	if entry, exists := b.cache[workloadID]; exists && time.Since(entry.Timestamp) < 10*time.Second {
		b.mu.Unlock()
		return entry.Nodes, entry.Edges, nil
	}
	b.mu.Unlock()

	// 1. Get root nodes from cognition.db
	rows, err := b.storage.CognitionDB.Query(`
		SELECT id, type, detail, timestamp FROM graph_nodes 
		WHERE detail LIKE ? LIMIT 10
	`, "%"+workloadID+"%")
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var nodes []GraphNode
	nodeMap := make(map[string]GraphNode)
	var queue []string

	for rows.Next() {
		var n GraphNode
		if err := rows.Scan(&n.ID, &n.Type, &n.Detail, &n.Timestamp); err == nil {
			nodes = append(nodes, n)
			nodeMap[n.ID] = n
			queue = append(queue, n.ID)
		}
	}

	// 2. BFS Traversal up to max depth of 3
	depth := 0
	visited := make(map[string]bool)
	for _, id := range queue {
		visited[id] = true
	}

	var edges []GraphEdge

	for len(queue) > 0 && depth < 3 {
		var nextQueue []string
		for _, fromID := range queue {
			// Query outbound edges
			edgeRows, err := b.storage.CognitionDB.Query(`
				SELECT from_node, to_node, relation_type FROM graph_edges 
				WHERE from_node = ?
			`, fromID)
			if err != nil {
				continue
			}

			for edgeRows.Next() {
				var e GraphEdge
				if err := edgeRows.Scan(&e.FromNode, &e.ToNode, &e.RelationType); err == nil {
					// Apply causal relevance pruning check
					// Prune edges linked to predictive simulation details if they are too deep
					if depth >= 2 && e.RelationType == "TRIGGERED_BY" {
						continue // Prune low-priority predictive branches
					}

					edges = append(edges, e)

					if !visited[e.ToNode] {
						visited[e.ToNode] = true
						// Fetch node details
						var child GraphNode
						err := b.storage.CognitionDB.QueryRow(`
							SELECT id, type, detail, timestamp FROM graph_nodes WHERE id = ?
						`, e.ToNode).Scan(&child.ID, &child.Type, &child.Detail, &child.Timestamp)
						if err == nil {
							nodes = append(nodes, child)
							nodeMap[child.ID] = child
							nextQueue = append(nextQueue, child.ID)
						}
					}
				}
			}
			edgeRows.Close()
		}
		queue = nextQueue
		depth++
	}

	// Cache result
	b.mu.Lock()
	b.cache[workloadID] = BGTECacheEntry{
		Nodes:     nodes,
		Edges:     edges,
		Timestamp: time.Now(),
	}
	b.mu.Unlock()

	return nodes, edges, nil
}

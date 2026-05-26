package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

type NodeCapabilities struct {
	NodeID      string    `json:"node_id"`
	TrustScore  float64   `json:"trust_score"`
	CPUCores    int       `json:"cpu_cores"`
	MemoryTotal int64     `json:"memory_total"`
	MemoryFree  int64     `json:"memory_free"`
	Temp        float64   `json:"temp"`
	Timestamp   time.Time `json:"timestamp"`
}

type BidProposal struct {
	WorkloadID string  `json:"workload_id"`
	NodeID     string  `json:"node_id"`
	BidScore   float64 `json:"bid_score"`
	Timestamp  time.Time `json:"timestamp"`
}

// setupSubscriptions hooks into NATS subjects for gossip and task negotiation
func (d *Daemon) setupSubscriptions() {
	if d.nc == nil {
		return
	}

	// 1. Listen for node gossip heartbeats
	d.nc.Subscribe("kalpana.sirl.domain.coordination.gossip.heartbeat", func(msg *nats.Msg) {
		var cap NodeCapabilities
		if err := json.Unmarshal(msg.Data, &cap); err != nil {
			return
		}
		if cap.NodeID == d.cfg.NodeID {
			return // Skip self
		}

		// Update database mesh nodes
		_, err := d.storage.RuntimeDB.Exec(`
			INSERT OR REPLACE INTO mesh_nodes (node_id, trust_score, cpu_cores, memory_total, status, last_seen)
			VALUES (?, ?, ?, ?, 'online', CURRENT_TIMESTAMP)
		`, cap.NodeID, cap.TrustScore, cap.CPUCores, cap.MemoryTotal)
		if err != nil {
			log.Printf("[FRML] Failed to cache mesh node %s: %v", cap.NodeID, err)
		}
	})

	// 2. Listen for deployment requests targeting this node
	deploySubject := fmt.Sprintf("kalpana.sirl.domain.runtime.commands.deploy.%s", d.cfg.NodeID)
	d.nc.Subscribe(deploySubject, func(msg *nats.Msg) {
		var spec WorkloadSpec
		if err := json.Unmarshal(msg.Data, &spec); err != nil {
			resp, _ := json.Marshal(map[string]string{"error": "invalid workload specs"})
			msg.Respond(resp)
			return
		}

		log.Printf("[FRML] Received deployment mandate from mesh for workload %s", spec.Name)
		cID, err := d.DeployWorkload(context.Background(), spec)
		if err != nil {
			resp, _ := json.Marshal(map[string]string{"error": err.Error()})
			msg.Respond(resp)
			return
		}

		resp, _ := json.Marshal(map[string]string{"container_id": cID})
		msg.Respond(resp)
	})

	// 3. Listen for global scheduling RFP bidding requests
	d.nc.Subscribe("kalpana.sirl.domain.coordination.proposal.request", func(msg *nats.Msg) {
		var rfp WorkloadSpec
		if err := json.Unmarshal(msg.Data, &rfp); err != nil {
			return
		}

		// Evaluate locally
		eval := d.EvaluateSuitability(context.Background(), rfp)
		log.Printf("[FRML] Bid evaluation for %s: score %.3f", rfp.Name, eval.TotalScore)

		// Submit bid if suitable
		bid := BidProposal{
			WorkloadID: rfp.ID,
			NodeID:     d.cfg.NodeID,
			BidScore:   eval.TotalScore,
			Timestamp:  time.Now(),
		}
		data, _ := json.Marshal(bid)
		_ = d.nc.Publish("kalpana.sirl.domain.coordination.proposal.bid", data)
	})

	// 4. Listen for incoming bid responses (for RFPs we initiated)
	d.nc.Subscribe("kalpana.sirl.domain.coordination.proposal.bid", func(msg *nats.Msg) {
		var bid BidProposal
		if err := json.Unmarshal(msg.Data, &bid); err != nil {
			return
		}

		d.bidMu.Lock()
		ch, exists := d.peerBids[bid.WorkloadID]
		d.bidMu.Unlock()

		if exists {
			select {
			case ch <- bid.BidScore:
			default:
				// Channel buffer full
			}
		}
	})

	log.Printf("[FRML] Subscribed to mesh federation topics.")
}

// NegotiateMeshPlacement negotiates placement across nodes using a reverse auction
func (d *Daemon) NegotiateMeshPlacement(ctx context.Context, spec WorkloadSpec) (string, error) {
	if d.nc == nil || !d.nc.IsConnected() {
		return "", fmt.Errorf("mesh offline")
	}

	// Create bid listener channel
	bidChan := make(chan float64, 10)
	d.bidMu.Lock()
	d.peerBids[spec.ID] = bidChan
	d.bidMu.Unlock()

	defer func() {
		d.bidMu.Lock()
		delete(d.peerBids, spec.ID)
		d.bidMu.Unlock()
	}()

	// Broadcast RFP
	data, _ := json.Marshal(spec)
	err := d.nc.Publish("kalpana.sirl.domain.coordination.proposal.request", data)
	if err != nil {
		return "", err
	}

	// Collect bids for 3 seconds
	timeout := time.After(3 * time.Second)
	bestScore := -1.0
	winningNode := d.cfg.NodeID // Default to self

	// Keep track of bids
	bidsMap := make(map[string]float64)

	for {
		select {
		case <-timeout:
			// Time is up, evaluate winning node
			log.Printf("[FRML] Bidding complete. Found %d bids.", len(bidsMap))
			for node, score := range bidsMap {
				if score > bestScore {
					bestScore = score
					winningNode = node
				}
			}
			if bestScore < 0.5 {
				return "", fmt.Errorf("no suitable bids received")
			}
			return winningNode, nil
		case score := <-bidChan:
			_ = score
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}

// Helper: query mesh nodes to resolve winning peer attributes
func (d *Daemon) getBestMeshNode(spec WorkloadSpec) (string, float64) {
	rows, err := d.storage.RuntimeDB.Query(`
		SELECT node_id, trust_score FROM mesh_nodes 
		WHERE status = 'online' AND last_seen > datetime('now', '-2 minutes')
	`)
	if err != nil {
		return "", 0.0
	}
	defer rows.Close()

	var bestNode string
	var bestScore float64

	for rows.Next() {
		var nid string
		var trust float64
		if err := rows.Scan(&nid, &trust); err == nil {
			if trust > bestScore {
				bestScore = trust
				bestNode = nid
			}
		}
	}
	return bestNode, bestScore
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type PeerInfo struct {
	DID          string    `json:"did"`
	FriendlyName string    `json:"friendly_name"`
	URL          string    `json:"url"`
	Status       string    `json:"status"` // online | offline
	AddedAt      time.Time `json:"added_at"`
}

type DelegateRequest struct {
	AgentID string `json:"agent_id"`
	Input   string `json:"input"`
	PlanID  string `json:"plan_id,omitempty"`
	StepID  string `json:"step_id,omitempty"`
}

type DelegateResponse struct {
	Output string `json:"output"`
	Error  string `json:"error,omitempty"`
}

// getNodeDID fetches this node's W3C DID from local SIL and caches it
func (s *Server) getNodeDID() (string, error) {
	s.didMu.Lock()
	defer s.didMu.Unlock()

	if s.nodeDID != "" {
		return s.nodeDID, nil
	}

	req, err := http.NewRequest("GET", s.cfg.SILURL+"/did/document", nil)
	if err != nil {
		return "", err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to fetch local did document: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("local SIL /did/document returned status %d", resp.StatusCode)
	}

	var doc struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil || doc.ID == "" {
		return "", fmt.Errorf("failed to decode local DID doc: %w", err)
	}

	s.nodeDID = doc.ID
	log.Printf("[federation] cached local node DID: %s", s.nodeDID)
	return s.nodeDID, nil
}

// getPeerAccessToken executes a challenge-response handshake to obtain/refresh peer token
func (s *Server) getPeerAccessToken(ctx context.Context, peerDID, peerURL, userToken string) (string, error) {
	s.tokenMu.Lock()
	cached, exists := s.peerTokens[peerDID]
	if exists && time.Now().Before(cached.ExpiresAt.Add(-30*time.Second)) {
		s.tokenMu.Unlock()
		return cached.Token, nil
	}
	s.tokenMu.Unlock()

	// 1. Get our own DID
	ownDID, err := s.getNodeDID()
	if err != nil {
		return "", fmt.Errorf("get own DID: %w", err)
	}

	// Clean trailing slash of peerURL if any
	baseURL := strings.TrimSuffix(peerURL, "/")

	// 2. Request challenge from peer's SIL (via peer's Orchestrator host but SIL port, or if they route to SIL port 8001 directly)
	// Wait, if peerURL is like "http://192.168.1.9:8006", the peer SIL is typically "http://192.168.1.9:8001".
	// Let's deduce the peer SIL URL from peerURL.
	// If peerURL contains :8006, replace with :8001. If not, append /did/auth/challenge.
	// In docker compose, each service is at "http://sil:8001". But across nodes, they use host IPs/ports.
	peerSILURL := baseURL
	if strings.Contains(baseURL, ":8006") {
		peerSILURL = strings.Replace(baseURL, ":8006", ":8001", 1)
	}

	chalReqBody, _ := json.Marshal(map[string]string{"did": ownDID})
	chalReq, err := http.NewRequestWithContext(ctx, "POST", peerSILURL+"/did/auth/challenge", bytes.NewBuffer(chalReqBody))
	if err != nil {
		return "", err
	}
	chalReq.Header.Set("Content-Type", "application/json")

	chalResp, err := s.client.Do(chalReq)
	if err != nil {
		return "", fmt.Errorf("peer challenge request failed at %s: %w", peerSILURL, err)
	}
	defer chalResp.Body.Close()

	if chalResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(chalResp.Body)
		return "", fmt.Errorf("peer challenge endpoint returned status %d: %s", chalResp.StatusCode, string(body))
	}

	var chalData struct {
		Challenge string `json:"challenge"`
		ExpiresAt string `json:"expires_at"`
	}
	if err := json.NewDecoder(chalResp.Body).Decode(&chalData); err != nil || chalData.Challenge == "" {
		return "", fmt.Errorf("failed to decode peer challenge: %w", err)
	}

	// 3. Sign the challenge using our local SIL
	signReqBody, _ := json.Marshal(map[string]string{"data": chalData.Challenge})
	signReq, err := http.NewRequestWithContext(ctx, "POST", s.cfg.SILURL+"/admin/sign", bytes.NewBuffer(signReqBody))
	if err != nil {
		return "", err
	}
	signReq.Header.Set("Content-Type", "application/json")
	if userToken != "" {
		signReq.Header.Set("Authorization", userToken)
	}

	signResp, err := s.client.Do(signReq)
	if err != nil {
		return "", fmt.Errorf("local SIL sign request failed: %w", err)
	}
	defer signResp.Body.Close()

	if signResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(signResp.Body)
		return "", fmt.Errorf("local SIL sign endpoint returned status %d: %s", signResp.StatusCode, string(body))
	}

	var signData struct {
		Signature string `json:"signature"`
	}
	if err := json.NewDecoder(signResp.Body).Decode(&signData); err != nil || signData.Signature == "" {
		return "", fmt.Errorf("failed to decode signature: %w", err)
	}

	// 4. Verify signature on peer to get access token
	verifyReqBody, _ := json.Marshal(map[string]string{
		"did":       ownDID,
		"signature": signData.Signature,
	})
	verifyReq, err := http.NewRequestWithContext(ctx, "POST", peerSILURL+"/did/auth/verify", bytes.NewBuffer(verifyReqBody))
	if err != nil {
		return "", err
	}
	verifyReq.Header.Set("Content-Type", "application/json")

	verifyResp, err := s.client.Do(verifyReq)
	if err != nil {
		return "", fmt.Errorf("peer verify request failed at %s: %w", peerSILURL, err)
	}
	defer verifyResp.Body.Close()

	if verifyResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(verifyResp.Body)
		return "", fmt.Errorf("peer verify endpoint returned status %d: %s", verifyResp.StatusCode, string(body))
	}

	var verifyData struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"` // seconds
	}
	if err := json.NewDecoder(verifyResp.Body).Decode(&verifyData); err != nil || verifyData.AccessToken == "" {
		return "", fmt.Errorf("failed to decode peer access token: %w", err)
	}

	// Cache the token
	expiresIn := 900
	if verifyData.ExpiresIn > 0 {
		expiresIn = verifyData.ExpiresIn
	}

	s.tokenMu.Lock()
	s.peerTokens[peerDID] = &CachedToken{
		Token:     verifyData.AccessToken,
		ExpiresAt: time.Now().Add(time.Duration(expiresIn) * time.Second),
	}
	s.tokenMu.Unlock()

	log.Printf("[federation] successfully completed handshake with peer %s", peerDID)
	return verifyData.AccessToken, nil
}

// delegateToPeerNode finds an online trusted peer and delegates the task to it
func (s *Server) delegateToPeerNode(ctx context.Context, agentID, input, planID, stepID string) (string, error) {
	userToken, _ := ctx.Value(tokenKey).(string)
	if userToken == "" {
		return "", errors.New("cannot delegate: missing user credentials in context")
	}

	// 1. Get trusted peers from local SIL
	req, err := http.NewRequestWithContext(ctx, "GET", s.cfg.SILURL+"/admin/peers", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", userToken)

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("failed to get trusted peers from local SIL: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("local SIL /admin/peers returned status %d: %s", resp.StatusCode, string(body))
	}

	var peerList struct {
		Peers []struct {
			DID          string `json:"did"`
			FriendlyName string `json:"friendly_name"`
			URL          string `json:"url"`
		} `json:"peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&peerList); err != nil {
		return "", fmt.Errorf("failed to decode peer list: %w", err)
	}

	if len(peerList.Peers) == 0 {
		return "", errors.New("no trusted peers configured")
	}

	var lastErr error
	for _, peer := range peerList.Peers {
		// Ping health
		healthURL := strings.TrimSuffix(peer.URL, "/") + "/health"
		pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
		pingReq, _ := http.NewRequestWithContext(pingCtx, "GET", healthURL, nil)
		pingResp, pingErr := s.client.Do(pingReq)
		pingCancel()

		if pingErr != nil || pingResp.StatusCode != http.StatusOK {
			if pingResp != nil {
				pingResp.Body.Close()
			}
			log.Printf("[federation] peer %s (%s) is offline", peer.FriendlyName, peer.URL)
			continue
		}
		pingResp.Body.Close()

		// Peer is online! Authenticate and delegate
		log.Printf("[federation] peer %s (%s) is online, executing handshake...", peer.FriendlyName, peer.URL)
		peerToken, handshakeErr := s.getPeerAccessToken(ctx, peer.DID, peer.URL, userToken)
		if handshakeErr != nil {
			log.Printf("[federation] handshake failed with peer %s: %v", peer.FriendlyName, handshakeErr)
			lastErr = handshakeErr
			continue
		}

		// Send delegation request
		delPayload := DelegateRequest{
			AgentID: agentID,
			Input:   input,
			PlanID:  planID,
			StepID:  stepID,
		}
		delBytes, _ := json.Marshal(delPayload)
		delegateURL := strings.TrimSuffix(peer.URL, "/") + "/peers/delegate"

		delCtx, delCancel := context.WithTimeout(ctx, 5*time.Minute)
		delReq, delErr := http.NewRequestWithContext(delCtx, "POST", delegateURL, bytes.NewBuffer(delBytes))
		if delErr != nil {
			delCancel()
			return "", delErr
		}
		delReq.Header.Set("Content-Type", "application/json")
		delReq.Header.Set("Authorization", "Bearer "+peerToken)

		delResp, delErr := s.client.Do(delReq)
		delCancel()

		if delErr != nil {
			log.Printf("[federation] delegation call failed for peer %s: %v", peer.FriendlyName, delErr)
			lastErr = delErr
			continue
		}
		defer delResp.Body.Close()

		if delResp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(delResp.Body)
			log.Printf("[federation] peer %s returned delegation error status %d: %s", peer.FriendlyName, delResp.StatusCode, string(body))
			lastErr = fmt.Errorf("peer returned status %d: %s", delResp.StatusCode, string(body))
			continue
		}

		var delData DelegateResponse
		if err := json.NewDecoder(delResp.Body).Decode(&delData); err != nil {
			lastErr = err
			continue
		}

		if delData.Error != "" {
			lastErr = errors.New(delData.Error)
			continue
		}

		log.Printf("[federation] workload successfully executed by peer %s", peer.FriendlyName)
		return delData.Output, nil
	}

	if lastErr != nil {
		return "", fmt.Errorf("all peers failed, last error: %w", lastErr)
	}
	return "", errors.New("no online peers available for delegation")
}

// GET /peers — list all trusted peers and check status
func (s *Server) handleGETPeers(w http.ResponseWriter, r *http.Request) {
	userToken, _ := r.Context().Value(tokenKey).(string)
	if userToken == "" {
		http.Error(w, `{"error":"missing authorization"}`, http.StatusUnauthorized)
		return
	}

	req, err := http.NewRequestWithContext(r.Context(), "GET", s.cfg.SILURL+"/admin/peers", nil)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	req.Header.Set("Authorization", userToken)

	resp, err := s.client.Do(req)
	if err != nil {
		http.Error(w, "SIL unreachable: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		http.Error(w, fmt.Sprintf("SIL returned %d: %s", resp.StatusCode, string(body)), resp.StatusCode)
		return
	}

	var rawPeers struct {
		Peers []struct {
			DID          string `json:"did"`
			FriendlyName string `json:"friendly_name"`
			URL          string `json:"url"`
			AddedAt      string `json:"added_at"`
		} `json:"peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rawPeers); err != nil {
		http.Error(w, "failed to decode peer list: "+err.Error(), http.StatusInternalServerError)
		return
	}

	peers := []PeerInfo{}
	for _, p := range rawPeers.Peers {
		status := "offline"
		// Ping health
		healthURL := strings.TrimSuffix(p.URL, "/") + "/health"
		pingCtx, pingCancel := context.WithTimeout(r.Context(), 1*time.Second)
		pingReq, _ := http.NewRequestWithContext(pingCtx, "GET", healthURL, nil)
		pingResp, pingErr := s.client.Do(pingReq)
		pingCancel()

		if pingErr == nil && pingResp.StatusCode == http.StatusOK {
			status = "online"
		}
		if pingResp != nil {
			pingResp.Body.Close()
		}

		added, _ := time.Parse(time.RFC3339, p.AddedAt)
		peers = append(peers, PeerInfo{
			DID:          p.DID,
			FriendlyName: p.FriendlyName,
			URL:          p.URL,
			Status:       status,
			AddedAt:      added,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"peers": peers, "total": len(peers)})
}

// POST /peers/delegate — execute delegated task from a peer node
func (s *Server) handlePOSTPeersDelegate(w http.ResponseWriter, r *http.Request) {
	// Verify peer role
	roles, ok := r.Context().Value(rolesKey).([]string)
	hasPeerOrAdmin := false
	if ok {
		for _, role := range roles {
			if role == "peer" || role == "admin" {
				hasPeerOrAdmin = true
				break
			}
		}
	}
	if !hasPeerOrAdmin {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]string{"error": "insufficient permissions: must be a trusted peer or admin"})
		return
	}

	var req DelegateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.AgentID == "" || req.Input == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "agent_id and input are required"})
		return
	}

	// Generate plan_id / step_id if missing
	planID := req.PlanID
	if planID == "" {
		planID = "delegated-" + newID()
	}
	stepID := req.StepID
	if stepID == "" {
		stepID = "step-" + newID()
	}

	log.Printf("[federation] executing delegated task: agent=%s, plan=%s, step=%s", req.AgentID, planID, stepID)

	// Execute task locally on this node's agents
	output, err := s.dispatchToAgent(r.Context(), req.AgentID, req.Input, planID, stepID)

	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		log.Printf("[federation] delegated task execution failed: %v", err)
		json.NewEncoder(w).Encode(DelegateResponse{Error: err.Error()})
		return
	}

	json.NewEncoder(w).Encode(DelegateResponse{Output: output})
}

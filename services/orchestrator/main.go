package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ─── Config ─────────────────────────────────────────────────────────────────

type Config struct {
	Port     string
	NATSURL  string
	SILURL   string
	AAFURL   string
	AICPURL  string
	SSIURL   string
	CertFile string
	KeyFile  string
	CAFile   string
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func loadConfig() Config {
	return Config{
		Port:     envOr("PORT", "8006"),
		NATSURL:  envOr("NATS_URL", "nats://localhost:4222"),
		SILURL:   envOr("SIL_URL", "http://sil:8001"),
		AAFURL:   envOr("AAF_URL", "http://aaf:8005"),
		AICPURL:  envOr("AICP_URL", "http://aicp:8004"),
		SSIURL:   envOr("SSI_URL", "http://ssi:8003"),
		CertFile: envOr("CERT_FILE", ""),
		KeyFile:  envOr("KEY_FILE", ""),
		CAFile:   envOr("CA_FILE", ""),
	}
}

// ─── mTLS ───────────────────────────────────────────────────────────────────

func buildTLSClient(certFile, keyFile, caFile string) *http.Client {
	if certFile == "" || keyFile == "" || caFile == "" {
		return &http.Client{Timeout: 30 * time.Second}
	}
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Printf("[mTLS] client cert load failed: %v — using plain HTTP", err)
		return &http.Client{Timeout: 30 * time.Second}
	}
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		log.Printf("[mTLS] CA cert load failed: %v — using plain HTTP", err)
		return &http.Client{Timeout: 30 * time.Second}
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{Certificates: []tls.Certificate{cert}, RootCAs: pool},
		},
	}
}

func buildTLSServer(srv *http.Server, certFile, keyFile, caFile string) (func() error, string) {
	if certFile == "" || keyFile == "" || caFile == "" {
		return func() error { return srv.ListenAndServe() }, "HTTP"
	}
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		log.Printf("[mTLS] CA load failed: %v — falling back to HTTP", err)
		return func() error { return srv.ListenAndServe() }, "HTTP"
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(caCert)
	srv.TLSConfig = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	return func() error { return srv.ListenAndServeTLS(certFile, keyFile) }, "mTLS"
}

// ─── Structs ─────────────────────────────────────────────────────────────────

type AgentManifest struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Description    string    `json:"description"`
	Capabilities   []string  `json:"capabilities"`
	Status         string    `json:"status"` // active | inactive | busy
	MaxConcurrency int       `json:"max_concurrency"`
	CurrentLoad    int       `json:"current_load"`
	LastHeartbeat  time.Time `json:"last_heartbeat"`
	RegisteredAt   time.Time `json:"registered_at"`
}

type OrchestrationStep struct {
	ID        string     `json:"id"`
	AgentID   string     `json:"agent_id"`
	Input     string     `json:"input"`
	DependsOn []string   `json:"depends_on"`
	Status    string     `json:"status"` // pending | running | completed | failed
	Output    string     `json:"output"`
	Error     string     `json:"error,omitempty"`
	StartedAt *time.Time `json:"started_at,omitempty"`
	EndedAt   *time.Time `json:"ended_at,omitempty"`
}

type OrchestrationPlan struct {
	ID          string              `json:"id"`
	Goal        string              `json:"goal"`
	Steps       []OrchestrationStep `json:"steps"`
	Status      string              `json:"status"` // pending | running | completed | failed
	FinalOutput string              `json:"final_output"`
	CreatedAt   time.Time           `json:"created_at"`
	CompletedAt *time.Time          `json:"completed_at,omitempty"`
}

type AgentMessage struct {
	TraceID   string          `json:"trace_id"`
	From      string          `json:"from"`
	To        string          `json:"to"`
	Type      string          `json:"type"` // task_request | task_result | heartbeat | register
	Payload   json.RawMessage `json:"payload"`
	ReplyTo   string          `json:"reply_to,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
}

type HeartbeatPayload struct {
	AgentSource string   `json:"agent_source"`
	Agents      []string `json:"agents"`
	Timestamp   time.Time `json:"timestamp"`
}

type RegisterPayload struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Description    string   `json:"description"`
	Capabilities   []string `json:"capabilities"`
	MaxConcurrency int      `json:"max_concurrency"`
}

type TaskResultPayload struct {
	StepID string `json:"step_id"`
	PlanID string `json:"plan_id"`
	Output string `json:"output"`
	Error  string `json:"error,omitempty"`
}

// ─── Server ──────────────────────────────────────────────────────────────────

type CachedToken struct {
	Token     string
	ExpiresAt time.Time
}

type Server struct {
	cfg        Config
	client     *http.Client
	nc         *nats.Conn
	mu         sync.RWMutex
	agents     map[string]*AgentManifest
	plans      map[string]*OrchestrationPlan
	planOrder  []string // insertion order for listing
	events     []AgentMessage
	eventMu    sync.RWMutex
	sseClients map[chan AgentMessage]struct{}
	sseMu      sync.Mutex

	// Federation
	nodeDID    string
	didMu      sync.Mutex
	peerTokens map[string]*CachedToken
	tokenMu    sync.Mutex

	// metrics
	orchTotal    *prometheus.CounterVec
	stepDuration *prometheus.HistogramVec
	agentsGauge  prometheus.Gauge
	orchDuration *prometheus.HistogramVec
}

func newServer(cfg Config) *Server {
	return &Server{
		cfg:        cfg,
		client:     buildTLSClient(cfg.CertFile, cfg.KeyFile, cfg.CAFile),
		agents:     make(map[string]*AgentManifest),
		plans:      make(map[string]*OrchestrationPlan),
		sseClients: make(map[chan AgentMessage]struct{}),
		peerTokens: make(map[string]*CachedToken),
		orchTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "kalpana_orchestrations_total",
			Help: "Total orchestration plans by status",
		}, []string{"status"}),
		stepDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kalpana_orchestration_step_duration_seconds",
			Help:    "Duration of individual orchestration steps",
			Buckets: prometheus.DefBuckets,
		}, []string{"agent_id"}),
		agentsGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "kalpana_agents_registered",
			Help: "Number of agents registered in the registry",
		}),
		orchDuration: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "kalpana_orchestration_duration_seconds",
			Help:    "Total orchestration plan duration",
			Buckets: prometheus.DefBuckets,
		}, []string{}),
	}
}

// ─── NATS ────────────────────────────────────────────────────────────────────

func (s *Server) connectNATS() {
	go func() {
		for attempt := 1; attempt <= 10; attempt++ {
			nc, err := nats.Connect(s.cfg.NATSURL,
				nats.RetryOnFailedConnect(true),
				nats.MaxReconnects(5),
				nats.ReconnectWait(3*time.Second),
			)
			if err != nil {
				log.Printf("[NATS] connect attempt %d/10 failed: %v — retrying in 3s", attempt, err)
				time.Sleep(3 * time.Second)
				continue
			}
			s.nc = nc
			log.Printf("[NATS] connected to %s", s.cfg.NATSURL)
			s.setupSubscriptions()
			return
		}
		log.Printf("[NATS] all connection attempts failed — orchestrator running without NATS")
	}()
}

func (s *Server) setupSubscriptions() {
	// Heartbeat from AAF and other sources
	s.nc.Subscribe("kalpana.agent.heartbeat", func(msg *nats.Msg) {
		var hb HeartbeatPayload
		if err := json.Unmarshal(msg.Data, &hb); err != nil {
			return
		}
		s.mu.Lock()
		for _, agentID := range hb.Agents {
			if a, ok := s.agents[agentID]; ok {
				a.LastHeartbeat = time.Now()
				a.Status = "active"
			}
		}
		s.mu.Unlock()
		s.broadcastEvent(AgentMessage{
			TraceID:   fmt.Sprintf("hb-%d", time.Now().UnixNano()),
			From:      hb.AgentSource,
			Type:      "heartbeat",
			Payload:   msg.Data,
			Timestamp: time.Now(),
		})
	})

	// Agent self-registration
	s.nc.Subscribe("kalpana.agent.register", func(msg *nats.Msg) {
		var reg RegisterPayload
		if err := json.Unmarshal(msg.Data, &reg); err != nil {
			return
		}
		s.registerAgent(reg)
	})

	// Task results from agents
	s.nc.Subscribe("kalpana.agent.*.results.*", func(msg *nats.Msg) {
		var result TaskResultPayload
		if err := json.Unmarshal(msg.Data, &result); err != nil {
			return
		}
		s.mu.Lock()
		if plan, ok := s.plans[result.PlanID]; ok {
			for i := range plan.Steps {
				if plan.Steps[i].ID == result.StepID {
					now := time.Now()
					plan.Steps[i].EndedAt = &now
					plan.Steps[i].Output = result.Output
					if result.Error != "" {
						plan.Steps[i].Status = "failed"
						plan.Steps[i].Error = result.Error
					} else {
						plan.Steps[i].Status = "completed"
					}
					break
				}
			}
		}
		s.mu.Unlock()
		s.broadcastEvent(AgentMessage{
			TraceID:   fmt.Sprintf("result-%s", result.StepID),
			From:      "agent",
			Type:      "task_result",
			Payload:   msg.Data,
			Timestamp: time.Now(),
		})
	})
}

func (s *Server) registerAgent(reg RegisterPayload) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mc := reg.MaxConcurrency
	if mc == 0 {
		mc = 5
	}
	if _, exists := s.agents[reg.ID]; !exists {
		s.agents[reg.ID] = &AgentManifest{
			ID:             reg.ID,
			Name:           reg.Name,
			Description:    reg.Description,
			Capabilities:   reg.Capabilities,
			Status:         "active",
			MaxConcurrency: mc,
			LastHeartbeat:  time.Now(),
			RegisteredAt:   time.Now(),
		}
		s.agentsGauge.Set(float64(len(s.agents)))
		log.Printf("[registry] registered agent: %s", reg.ID)
	} else {
		a := s.agents[reg.ID]
		a.LastHeartbeat = time.Now()
		a.Status = "active"
		if reg.Capabilities != nil {
			a.Capabilities = reg.Capabilities
		}
	}
}

// Health monitor — mark stale agents as inactive
func (s *Server) startHealthMonitor() {
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			s.mu.Lock()
			threshold := time.Now().Add(-90 * time.Second)
			for _, a := range s.agents {
				if a.LastHeartbeat.Before(threshold) && a.Status == "active" {
					a.Status = "inactive"
					log.Printf("[registry] agent %s marked inactive (no heartbeat)", a.ID)
				}
			}
			s.mu.Unlock()
		}
	}()
}

// ─── SSE Event Broadcasting ──────────────────────────────────────────────────

func (s *Server) broadcastEvent(ev AgentMessage) {
	s.eventMu.Lock()
	s.events = append(s.events, ev)
	if len(s.events) > 100 {
		s.events = s.events[len(s.events)-100:]
	}
	s.eventMu.Unlock()

	s.sseMu.Lock()
	for ch := range s.sseClients {
		select {
		case ch <- ev:
		default:
		}
	}
	s.sseMu.Unlock()
}

// ─── Orchestration Engine ─────────────────────────────────────────────────────

func newID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func (s *Server) decomposeGoal(goal string) []OrchestrationStep {
	lower := strings.ToLower(goal)
	var steps []OrchestrationStep
	addStep := func(agentID, input string, deps []string) {
		id := fmt.Sprintf("step-%d", len(steps)+1)
		steps = append(steps, OrchestrationStep{
			ID:        id,
			AgentID:   agentID,
			Input:     input,
			DependsOn: deps,
			Status:    "pending",
		})
	}

	// Comprehensive analysis — all agents
	if strings.Contains(lower, "comprehensive") || strings.Contains(lower, "full analysis") ||
		strings.Contains(lower, "synthesize") || strings.Contains(lower, "everything") {
		addStep("infrareportagent", goal, nil)
		addStep("anomalydetectionagent", goal, nil)
		addStep("metricanalysisagent", goal, nil)
		addStep("predictivescalingagent", "{{step_3_output}}", []string{"step-3"})
		addStep("knowledgesynthesisagent", goal+" context:{{step_1_output}} anomalies:{{step_2_output}}", []string{"step-1", "step-2"})
		return steps
	}

	var firstStepID string

	if strings.Contains(lower, "report") || strings.Contains(lower, "status") || strings.Contains(lower, "overview") {
		addStep("infrareportagent", goal, nil)
		firstStepID = "step-1"
	}

	if strings.Contains(lower, "anomaly") || strings.Contains(lower, "detect") || strings.Contains(lower, "scan") || strings.Contains(lower, "health") {
		addStep("anomalydetectionagent", goal, nil)
	}

	if strings.Contains(lower, "metric") || strings.Contains(lower, "performance") || strings.Contains(lower, "usage") || strings.Contains(lower, "cpu") || strings.Contains(lower, "memory") {
		addStep("metricanalysisagent", goal, nil)
	}

	if strings.Contains(lower, "scale") || strings.Contains(lower, "predict") || strings.Contains(lower, "recommend") || strings.Contains(lower, "optim") {
		var deps []string
		input := goal
		if firstStepID != "" {
			deps = []string{firstStepID}
			input = "{{" + firstStepID + "_output}}"
		}
		addStep("predictivescalingagent", input, deps)
	}

	if strings.Contains(lower, "search") || strings.Contains(lower, "knowledge") || strings.Contains(lower, "find") || strings.Contains(lower, "document") {
		addStep("searchagent", goal, nil)
	}

	if strings.Contains(lower, "knowledge") && (strings.Contains(lower, "synth") || strings.Contains(lower, "compile")) {
		addStep("knowledgesynthesisagent", goal, nil)
	}

	if len(steps) == 0 {
		// Default: report + anomaly
		addStep("infrareportagent", goal, nil)
		addStep("anomalydetectionagent", goal, nil)
	}

	return steps
}

func (s *Server) runOrchestration(ctx context.Context, plan *OrchestrationPlan) {
	start := time.Now()
	plan.Status = "running"

	// Topological execution — process steps respecting dependencies
	completed := map[string]string{} // stepID -> output
	completedMu := sync.Mutex{}

	for {
		// Find steps that are ready to run (pending and all deps satisfied)
		var ready []int
		completedMu.Lock()
		for i, step := range plan.Steps {
			if step.Status == "pending" {
				depsOK := true
				for _, dep := range step.DependsOn {
					if _, ok := completed[dep]; !ok {
						depsOK = false
						break
					}
				}
				if depsOK {
					ready = append(ready, i)
				}
			}
		}
		completedMu.Unlock()

		hasRunning := false
		s.mu.Lock()
		for _, step := range plan.Steps {
			if step.Status == "running" {
				hasRunning = true
				break
			}
		}
		s.mu.Unlock()

		if len(ready) == 0 {
			if hasRunning {
				// Waiting for running steps to finish
				time.Sleep(500 * time.Millisecond)
				continue
			} else {
				// No ready steps and no running steps -> blocked!
				s.mu.Lock()
				for i := range plan.Steps {
					if plan.Steps[i].Status == "pending" {
						plan.Steps[i].Status = "failed"
						plan.Steps[i].Error = "dependency failed"
					}
				}
				s.mu.Unlock()
				break
			}
		}

		// Mark ready steps as running
		s.mu.Lock()
		for _, idx := range ready {
			plan.Steps[idx].Status = "running"
			now := time.Now()
			plan.Steps[idx].StartedAt = &now
		}
		s.mu.Unlock()

		// Run ready steps in parallel
		var wg sync.WaitGroup
		for _, idx := range ready {
			wg.Add(1)
			go func(stepIdx int) {
				defer wg.Done()
				step := &plan.Steps[stepIdx]
				stepStart := time.Now()

				// Replace placeholders in input
				completedMu.Lock()
				input := step.Input
				for sid, out := range completed {
					placeholder := "{{" + sid + "_output}}"
					input = strings.ReplaceAll(input, placeholder, out)
					// Also support {{step_N_output}} where N is step number
					num := strings.TrimPrefix(sid, "step-")
					input = strings.ReplaceAll(input, "{{step_"+num+"_output}}", out)
				}
				completedMu.Unlock()

				output, err := s.dispatchToAgent(ctx, step.AgentID, input, plan.ID, step.ID)

				s.mu.Lock()
				now := time.Now()
				step.EndedAt = &now
				if err != nil {
					step.Status = "failed"
					step.Error = err.Error()
				} else {
					step.Status = "completed"
					step.Output = output
				}
				s.mu.Unlock()

				completedMu.Lock()
				if err == nil {
					completed[step.ID] = output
				}
				completedMu.Unlock()

				s.stepDuration.WithLabelValues(step.AgentID).Observe(time.Since(stepStart).Seconds())

				s.broadcastEvent(AgentMessage{
					TraceID:   plan.ID,
					From:      "orchestrator",
					To:        step.AgentID,
					Type:      "task_result",
					Timestamp: time.Now(),
				})
			}(idx)
		}
		wg.Wait()
	}

	// Synthesize final output
	var parts []string
	for _, step := range plan.Steps {
		if step.Output != "" {
			parts = append(parts, fmt.Sprintf("## %s Result\n\n%s", step.AgentID, step.Output))
		}
	}
	plan.FinalOutput = strings.Join(parts, "\n\n---\n\n")
	now := time.Now()
	plan.CompletedAt = &now

	// Check if any step failed
	allOK := true
	for _, step := range plan.Steps {
		if step.Status == "failed" {
			allOK = false
			break
		}
	}
	if allOK {
		plan.Status = "completed"
		s.orchTotal.WithLabelValues("completed").Inc()
	} else {
		plan.Status = "failed"
		s.orchTotal.WithLabelValues("failed").Inc()
	}
	s.orchDuration.WithLabelValues().Observe(time.Since(start).Seconds())

	log.Printf("[orch] plan %s finished with status=%s in %.2fs", plan.ID, plan.Status, time.Since(start).Seconds())
}

func (s *Server) dispatchToAgent(ctx context.Context, agentID, input, planID, stepID string) (string, error) {
	// Check capacity/local status
	var shouldDelegate bool
	s.mu.RLock()
	agent, exists := s.agents[agentID]
	if !exists || agent.Status == "inactive" || agent.CurrentLoad >= agent.MaxConcurrency {
		shouldDelegate = true
	}
	s.mu.RUnlock()

	// If missing or busy, and this is not a loop, try peer delegation
	if shouldDelegate && !strings.HasPrefix(planID, "delegated-") {
		log.Printf("[federation] agent %s busy/missing locally, attempting delegation to peer", agentID)
		output, err := s.delegateToPeerNode(ctx, agentID, input, planID, stepID)
		if err == nil {
			return output, nil
		}
		log.Printf("[federation] peer delegation failed: %v, falling back to local stack", err)
	}

	// Try NATS first if connected
	if s.nc != nil && s.nc.IsConnected() {
		msg := AgentMessage{
			TraceID:   fmt.Sprintf("%s-%s", planID, stepID),
			From:      "orchestrator",
			To:        agentID,
			Type:      "task_request",
			Timestamp: time.Now(),
		}
		payload := map[string]string{"input": input, "plan_id": planID, "step_id": stepID}
		raw, _ := json.Marshal(payload)
		msg.Payload = raw
		msgBytes, _ := json.Marshal(msg)

		replySubject := fmt.Sprintf("kalpana.orch.reply.%s.%s", planID, stepID)
		sub, err := s.nc.SubscribeSync(replySubject)
		if err == nil {
			defer sub.Unsubscribe()
			subject := fmt.Sprintf("kalpana.agent.%s.tasks", agentID)
			msgObj := &nats.Msg{Subject: subject, Reply: replySubject, Data: msgBytes}
			if pubErr := s.nc.PublishMsg(msgObj); pubErr == nil {
				natsMsg, natsErr := sub.NextMsgWithContext(ctx)
				if natsErr == nil {
					var result TaskResultPayload
					if jsonErr := json.Unmarshal(natsMsg.Data, &result); jsonErr == nil {
						if result.Error != "" {
							return "", errors.New(result.Error)
						}
						return result.Output, nil
					}
				}
			}
		}
	}

	// Fallback: call AAF HTTP API directly
	return s.dispatchViaAAF(ctx, agentID, input)
}

func (s *Server) dispatchViaAAF(ctx context.Context, agentID, input string) (string, error) {
	body, _ := json.Marshal(map[string]string{"agent_id": agentID, "input": input})
	req, _ := http.NewRequestWithContext(ctx, "POST", s.cfg.AAFURL+"/tasks", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	if token, ok := ctx.Value(tokenKey).(string); ok && token != "" {
		req.Header.Set("Authorization", token)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("AAF dispatch error: %w", err)
	}
	defer resp.Body.Close()

	var taskResp struct {
		TaskID string `json:"task_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&taskResp); err != nil || taskResp.TaskID == "" {
		return "", errors.New("failed to get task_id from AAF")
	}

	// Poll for completion
	deadline := time.Now().Add(5 * time.Minute)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(3 * time.Second):
		}
		pollReq, _ := http.NewRequestWithContext(ctx, "GET", s.cfg.AAFURL+"/tasks/"+taskResp.TaskID, nil)
		if token, ok := ctx.Value(tokenKey).(string); ok && token != "" {
			pollReq.Header.Set("Authorization", token)
		}
		pollResp, err := s.client.Do(pollReq)
		if err != nil {
			continue
		}
		var task struct {
			Status string `json:"status"`
			Output string `json:"output"`
			Error  string `json:"error"`
		}
		json.NewDecoder(pollResp.Body).Decode(&task)
		pollResp.Body.Close()
		if strings.EqualFold(task.Status, "completed") {
			return task.Output, nil
		}
		if strings.EqualFold(task.Status, "failed") {
			return "", errors.New(task.Error)
		}
	}
	return "", errors.New("agent task timed out after 5 minutes")
}

// ─── JWT Middleware ──────────────────────────────────────────────────────────

type contextKey string
const tokenKey contextKey = "token"
const rolesKey contextKey = "roles"
const emailKey contextKey = "email"

func (s *Server) jwtMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "missing token"})
			return
		}
		req, _ := http.NewRequestWithContext(r.Context(), "GET", s.cfg.SILURL+"/validate", nil)
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := s.client.Do(req)
		if err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{"error": "auth service unavailable"})
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
			return
		}

		var valResp struct {
			Valid bool     `json:"valid"`
			Email string   `json:"email"`
			Roles []string `json:"roles"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&valResp); err != nil || !valResp.Valid {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]string{"error": "invalid or expired token"})
			return
		}

		ctx := context.WithValue(r.Context(), tokenKey, "Bearer "+token)
		ctx = context.WithValue(ctx, rolesKey, valResp.Roles)
		ctx = context.WithValue(ctx, emailKey, valResp.Email)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// ─── Handlers ────────────────────────────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	agentCount := len(s.agents)
	s.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":             "ok",
		"service":            "orchestrator",
		"agents_registered":  agentCount,
		"nats_connected":     s.nc != nil && s.nc.IsConnected(),
	})
}

func (s *Server) handleRegisterAgent(w http.ResponseWriter, r *http.Request) {
	var reg RegisterPayload
	if err := json.NewDecoder(r.Body).Decode(&reg); err != nil || reg.ID == "" {
		http.Error(w, `{"error":"invalid payload"}`, http.StatusBadRequest)
		return
	}
	s.registerAgent(reg)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "registered", "id": reg.ID})
}

func (s *Server) handleListAgents(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	agents := make([]*AgentManifest, 0, len(s.agents))
	for _, a := range s.agents {
		agents = append(agents, a)
	}
	s.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"agents": agents, "total": len(agents)})
}

func (s *Server) handleGetAgent(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	s.mu.RLock()
	a, ok := s.agents[id]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, `{"error":"agent not found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(a)
}

func (s *Server) handleOrchestrate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Goal    string `json:"goal"`
		Context string `json:"context,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Goal == "" {
		http.Error(w, `{"error":"goal required"}`, http.StatusBadRequest)
		return
	}

	planID := newID()
	steps := s.decomposeGoal(req.Goal)
	if req.Context != "" {
		for i := range steps {
			if steps[i].Input == "" || steps[i].Input == req.Goal {
				steps[i].Input = req.Goal + "\n\nContext: " + req.Context
			}
		}
	}

	plan := &OrchestrationPlan{
		ID:        planID,
		Goal:      req.Goal,
		Steps:     steps,
		Status:    "pending",
		CreatedAt: time.Now(),
	}

	s.mu.Lock()
	s.plans[planID] = plan
	s.planOrder = append(s.planOrder, planID)
	if len(s.planOrder) > 100 {
		oldest := s.planOrder[0]
		s.planOrder = s.planOrder[1:]
		delete(s.plans, oldest)
	}
	s.mu.Unlock()

	s.broadcastEvent(AgentMessage{
		TraceID: planID, From: "orchestrator", Type: "plan_started",
		Timestamp: time.Now(),
	})

	// Run synchronously (with timeout)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()
	s.runOrchestration(ctx, plan)

	w.Header().Set("Content-Type", "application/json")
	s.mu.RLock()
	defer s.mu.RUnlock()
	json.NewEncoder(w).Encode(plan)
}

func (s *Server) handleListPlans(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	plans := make([]*OrchestrationPlan, 0, len(s.planOrder))
	for i := len(s.planOrder) - 1; i >= 0 && i >= len(s.planOrder)-50; i-- {
		if p, ok := s.plans[s.planOrder[i]]; ok {
			plans = append(plans, p)
		}
	}
	s.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"plans": plans, "total": len(plans)})
}

func (s *Server) handleGetPlan(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	s.mu.RLock()
	p, ok := s.plans[id]
	s.mu.RUnlock()
	if !ok {
		http.Error(w, `{"error":"plan not found"}`, http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(p)
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "SSE not supported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	// Send buffered events
	s.eventMu.RLock()
	buffered := make([]AgentMessage, len(s.events))
	copy(buffered, s.events)
	s.eventMu.RUnlock()
	for _, ev := range buffered {
		data, _ := json.Marshal(ev)
		fmt.Fprintf(w, "data: %s\n\n", data)
	}
	flusher.Flush()

	// Register client
	ch := make(chan AgentMessage, 32)
	s.sseMu.Lock()
	s.sseClients[ch] = struct{}{}
	s.sseMu.Unlock()
	defer func() {
		s.sseMu.Lock()
		delete(s.sseClients, ch)
		s.sseMu.Unlock()
	}()

	// Stream live events
	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-ch:
			data, _ := json.Marshal(ev)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		case <-time.After(30 * time.Second):
			// Keepalive
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

// Proxy: expose AAF /agents list for UI convenience
func (s *Server) handleAAFAgents(w http.ResponseWriter, r *http.Request) {
	req, _ := http.NewRequestWithContext(r.Context(), "GET", s.cfg.AAFURL+"/agents", nil)
	if auth := r.Header.Get("Authorization"); auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		http.Error(w, `{"error":"AAF unreachable"}`, http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// ─── Router ──────────────────────────────────────────────────────────────────

func (s *Server) routes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Access-Control-Allow-Origin", "*")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
			if req.Method == "OPTIONS" {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			next.ServeHTTP(w, req)
		})
	})

	r.Get("/health", s.handleHealth)
	r.Handle("/metrics", promhttp.Handler())
	r.Get("/events", s.handleSSE) // SSE — no JWT required for dashboard
	r.Post("/agents/register", s.handleRegisterAgent) // Public registration endpoint for agents

	r.Group(func(r chi.Router) {
		r.Use(s.jwtMiddleware)
		r.Get("/agents", s.handleListAgents)
		r.Get("/agents/{id}", s.handleGetAgent)
		r.Get("/agents/aaf", s.handleAAFAgents)
		r.Post("/orchestrate", s.handleOrchestrate)
		r.Get("/plans", s.handleListPlans)
		r.Get("/plans/{id}", s.handleGetPlan)
		r.Get("/peers", s.handleGETPeers)
		r.Post("/peers/delegate", s.handlePOSTPeersDelegate)
	})

	return r
}

// ─── Main ────────────────────────────────────────────────────────────────────

func main() {
	cfg := loadConfig()
	srv := newServer(cfg)

	srv.connectNATS()
	srv.startHealthMonitor()

	addr := ":" + cfg.Port
	httpSrv := &http.Server{
		Addr:         addr,
		Handler:      srv.routes(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Minute, // long for SSE + orchestration
		IdleTimeout:  60 * time.Second,
	}

	serve, mode := buildTLSServer(httpSrv, cfg.CertFile, cfg.KeyFile, cfg.CAFile)
	log.Printf("[INFO] KalpanaOS Orchestrator starting on %s (%s)", addr, mode)
	if err := serve(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Server fatal error: %v", err)
	}
}

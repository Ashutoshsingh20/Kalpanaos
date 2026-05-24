package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/golang-jwt/jwt/v5"
	_ "github.com/mattn/go-sqlite3"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ─── Config ──────────────────────────────────────────────────────────────────

type Config struct {
	Port           string
	DBPath         string
	SILURL         string
	SSIURL         string
	COLURL         string
	AICPURL        string
	NATSURL        string
	// Phase 3
	ORCHESTRATORURL string
	NvidiaAPIKey    string
	NvidiaAPIBase   string
	NvidiaChatModel string
	PrometheusURL   string
	CertFile        string
	KeyFile         string
	CAFile          string
	FDCLURL         string
}

func loadConfig() Config {
	return Config{
		Port:            getEnv("PORT", "8005"),
		DBPath:          getEnv("DB_PATH", "/data/aaf.db"),
		SILURL:          getEnv("SIL_URL", "http://sil:8001"),
		SSIURL:          getEnv("SSI_URL", "http://ssi:8003"),
		COLURL:          getEnv("COL_URL", "http://col:8002"),
		AICPURL:         getEnv("AICP_URL", "http://aicp:8004"),
		NATSURL:         getEnv("NATS_URL", "nats://nats:4222"),
		// Phase 3
		ORCHESTRATORURL: getEnv("ORCHESTRATOR_URL", "http://orchestrator:8006"),
		NvidiaAPIKey:    getEnv("NVIDIA_API_KEY", "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"),
		NvidiaAPIBase:   getEnv("NVIDIA_API_BASE_URL", "https://integrate.api.nvidia.com/v1"),
		NvidiaChatModel: getEnv("NVIDIA_CHAT_MODEL", "meta/llama-3.1-8b-instruct"),
		PrometheusURL:   getEnv("PROMETHEUS_URL", "http://prometheus:9090"),
		FDCLURL:         getEnv("FDCL_URL", "http://fdcl:8009"),
		CertFile:        getEnv("CERT_FILE", ""),
		KeyFile:         getEnv("KEY_FILE", ""),
		CAFile:          getEnv("CA_FILE", ""),
	}
}

func getEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ─── Models ───────────────────────────────────────────────────────────────────

type Agent struct {
	ID           string    `json:"id"`
	Name         string    `json:"name"`
	Description  string    `json:"description"`
	Capabilities []string  `json:"capabilities"`
	Status       string    `json:"status"`
	RegisteredAt time.Time `json:"registered_at"`
}

type Task struct {
	ID          string     `json:"id"`
	AgentID     string     `json:"agent_id"`
	Status      string     `json:"status"`
	Input       string     `json:"input"`
	Output      string     `json:"output,omitempty"`
	Error       string     `json:"error,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
	RetryCount  int        `json:"retry_count"`
}

type Checkpoint struct {
	ID        string    `json:"id"`
	TaskID    string    `json:"task_id"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
}

// ─── Request/Response types ───────────────────────────────────────────────────

type RegisterAgentRequest struct {
	Name         string   `json:"name"`
	Description  string   `json:"description"`
	Capabilities []string `json:"capabilities"`
}

type SubmitTaskRequest struct {
	AgentID string `json:"agent_id"`
	Input   string `json:"input"`
}

type SubmitTaskResponse struct {
	TaskID string `json:"task_id"`
	Status string `json:"status"`
}

type NATSTaskMessage struct {
	AgentID string `json:"agent_id"`
	Input   string `json:"input"`
}

// ─── Prometheus Metrics ───────────────────────────────────────────────────────

var (
	tasksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "aaf_tasks_total",
		Help: "Total number of tasks submitted",
	}, []string{"agent_id", "status"})

	taskDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "aaf_task_duration_seconds",
		Help:    "Task execution duration in seconds",
		Buckets: prometheus.DefBuckets,
	}, []string{"agent_id"})

	agentsRegistered = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "aaf_agents_registered_total",
		Help: "Total number of registered agents",
	})

	httpRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "aaf_http_requests_total",
		Help: "Total HTTP requests",
	}, []string{"method", "path", "status"})
)

func init() {
	prometheus.MustRegister(tasksTotal, taskDuration, agentsRegistered, httpRequests)
}

// ─── Server ───────────────────────────────────────────────────────────────────

type Server struct {
	cfg        Config
	db         *sql.DB
	nc         *nats.Conn
	httpClient *http.Client

	// In-memory cancel map for running tasks
	cancelMu sync.Mutex
	cancels  map[string]context.CancelFunc
}

func newServer(cfg Config) (*Server, error) {
	db, err := sql.Open("sqlite3", cfg.DBPath+"?_journal=WAL&_timeout=5000&_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(1)

	s := &Server{
		cfg:        cfg,
		db:         db,
		httpClient: buildAAFTLSClient(cfg.CertFile, cfg.KeyFile, cfg.CAFile),
		cancels:    make(map[string]context.CancelFunc),
	}
	return s, nil
}


func buildAAFTLSClient(certFile, keyFile, caFile string) *http.Client {
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

func buildAAFTLSServer(srv *http.Server, certFile, keyFile, caFile string) (func() error, string) {
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

// ─── NVIDIA Direct Caller ────────────────────────────────────────────────────

type nvidiaMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func (s *Server) callNVIDIA(ctx context.Context, systemPrompt, userPrompt string) (string, error) {
	body, _ := json.Marshal(map[string]interface{}{
		"model": s.cfg.NvidiaChatModel,
		"messages": []nvidiaMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: userPrompt},
		},
		"temperature": 0.3,
		"max_tokens":  2048,
		"stream":      false,
	})
	req, err := http.NewRequestWithContext(ctx, "POST",
		s.cfg.NvidiaAPIBase+"/chat/completions",
		bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.cfg.NvidiaAPIKey)

	client := &http.Client{Timeout: 2 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("NVIDIA API error: %w", err)
	}
	defer resp.Body.Close()

	var result struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("NVIDIA response decode: %w", err)
	}
	if len(result.Choices) == 0 {
		return "", fmt.Errorf("NVIDIA returned no choices")
	}
	return result.Choices[0].Message.Content, nil
}

// ─── SchedulerAgent (Phase 5.5) ───────────────────────────────────────────────

func (s *Server) runSchedulerAgent(ctx context.Context, input string) (string, error) {
	// Parse input as DeployRequest
	var req struct {
		Name     string            `json:"name"`
		Image    string            `json:"image"`
		Ports    []string          `json:"ports"`
		Env      []string          `json:"env"`
		MemLimit int64             `json:"mem_limit"`
		Labels   map[string]string `json:"labels"`
		Restart  string            `json:"restart"`
	}
	if err := json.Unmarshal([]byte(input), &req); err != nil {
		return "", fmt.Errorf("invalid deployment manifest: %w", err)
	}

	// 1. Query IKG for nodes
	ikgReq, _ := http.NewRequestWithContext(ctx, "GET", "http://ikg:8008/query", nil)
	ikgResp, err := http.DefaultClient.Do(ikgReq)
	var nodes []struct {
		ID         string            `json:"id"`
		Type       string            `json:"type"`
		Properties map[string]string `json:"properties"`
	}
	if err == nil && ikgResp.StatusCode == http.StatusOK {
		json.NewDecoder(ikgResp.Body).Decode(&nodes)
		ikgResp.Body.Close()
	}

	// For MVP (4GB single node), we simply select the local node (since the current setup is single-node).
	// In a real multi-node setup, we'd iterate over `nodes` where `Type == "NODE"`, fetch their telemetry via Prometheus,
	// and select the one with the lowest CPU/Memory usage.
	
	// Determine local node ID (we can grab it from env or assume hostname)
	host, _ := os.Hostname()
	if host == "" {
		host = "unknown"
	}
	targetNodeID := getEnv("NODE_ID", host)

	// Publish directly to target node's COL NATS topic
	subject := fmt.Sprintf("kalpana.col.deploy.%s", targetNodeID)
	payload, _ := json.Marshal(req)
	
	// Wait for an ack? We don't have a reply expected in NATS subscribe yet, but we can just fire and forget or expect error logs.
	if err := s.nc.Publish(subject, payload); err != nil {
		return "", fmt.Errorf("failed to schedule deployment to %s: %w", targetNodeID, err)
	}

	return fmt.Sprintf("Deployment for %s successfully scheduled to node %s", req.Name, targetNodeID), nil
}

// ─── MemoryCompressionAgent (Phase 5.6) ────────────────────────────────────────

func (s *Server) runMemoryCompressionAgent(ctx context.Context, input string) (string, error) {
	tokenStr, _ := ctx.Value(contextKeyToken).(string)
	
	// 1. Fetch old memories
	req, _ := http.NewRequestWithContext(ctx, "GET", s.cfg.SSIURL+"/memory?limit=50", nil)
	if tokenStr != "" {
		req.Header.Set("Authorization", "Bearer "+tokenStr)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch memories: %w", err)
	}
	defer resp.Body.Close()

	var memData struct {
		Memories []struct {
			ID        string `json:"id"`
			AgentID   string `json:"agent_id"`
			Status    string `json:"status"`
			Input     string `json:"input"`
			Output    string `json:"output"`
			Timestamp int64  `json:"timestamp"`
		} `json:"memories"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&memData); err != nil {
		return "", fmt.Errorf("decode memories: %w", err)
	}

	if len(memData.Memories) == 0 {
		return "No memories to compress.", nil
	}

	// 2. Synthesize using NVIDIA LLM
	rawMemories, _ := json.Marshal(memData.Memories)
	prompt := fmt.Sprintf(
		"You are KalpanaOS's memory compression engine. Synthesize the following raw episodic memories into a single concise 'Abstracted Insight' paragraph that captures the core lessons learned. Omit raw data, focus on patterns.\n\nMemories:\n%s",
		string(rawMemories),
	)

	llmPayload, _ := json.Marshal(map[string]interface{}{
		"model":       os.Getenv("NVIDIA_CHAT_MODEL"),
		"messages":    []map[string]string{{"role": "user", "content": prompt}},
		"temperature": 0.3,
		"max_tokens":  500,
	})

	nvidiaReq, _ := http.NewRequestWithContext(ctx, "POST", os.Getenv("NVIDIA_API_BASE_URL")+"/chat/completions", bytes.NewReader(llmPayload))
	nvidiaReq.Header.Set("Authorization", "Bearer "+os.Getenv("NVIDIA_API_KEY"))
	nvidiaReq.Header.Set("Content-Type", "application/json")

	nvidiaResp, err := http.DefaultClient.Do(nvidiaReq)
	if err != nil {
		return "", fmt.Errorf("nvidia api call: %w", err)
	}
	defer nvidiaResp.Body.Close()

	var llmResult struct {
		Choices []struct {
			Message struct {
				Content string `json:"content"`
			} `json:"message"`
		} `json:"choices"`
	}
	if err := json.NewDecoder(nvidiaResp.Body).Decode(&llmResult); err != nil {
		return "", fmt.Errorf("nvidia decode: %w", err)
	}
	if len(llmResult.Choices) == 0 {
		return "", fmt.Errorf("no summary generated")
	}
	summary := llmResult.Choices[0].Message.Content

	// 3. Save new dense memory
	newMem := map[string]interface{}{
		"agent_id": "System",
		"status":   "COMPRESSED",
		"input":    "Compression Cycle",
		"output":   summary,
	}
	savePayload, _ := json.Marshal(newMem)
	saveReq, _ := http.NewRequestWithContext(ctx, "POST", s.cfg.SSIURL+"/memory", bytes.NewReader(savePayload))
	if tokenStr != "" {
		saveReq.Header.Set("Authorization", "Bearer "+tokenStr)
	}
	saveReq.Header.Set("Content-Type", "application/json")
	if r, err := http.DefaultClient.Do(saveReq); err == nil {
		r.Body.Close()
	}

	// 4. Delete old memories
	deletedCount := 0
	for _, m := range memData.Memories {
		delReq, _ := http.NewRequestWithContext(ctx, "DELETE", fmt.Sprintf("%s/memory/%s", s.cfg.SSIURL, m.ID), nil)
		if tokenStr != "" {
			delReq.Header.Set("Authorization", "Bearer "+tokenStr)
		}
		if dr, err := http.DefaultClient.Do(delReq); err == nil {
			dr.Body.Close()
			deletedCount++
		}
	}

	return fmt.Sprintf("Successfully compressed %d raw memories into 1 insight: %s", deletedCount, summary), nil
}


// ─── DB Init ──────────────────────────────────────────────────────────────────

func (s *Server) initDB() error {
	schema := `
	CREATE TABLE IF NOT EXISTS agents (
		id            TEXT PRIMARY KEY,
		name          TEXT NOT NULL UNIQUE,
		description   TEXT NOT NULL,
		capabilities  TEXT NOT NULL DEFAULT '[]',
		status        TEXT NOT NULL DEFAULT 'active',
		registered_at DATETIME NOT NULL
	);

	CREATE TABLE IF NOT EXISTS tasks (
		id           TEXT PRIMARY KEY,
		agent_id     TEXT NOT NULL,
		status       TEXT NOT NULL DEFAULT 'PENDING',
		input        TEXT NOT NULL DEFAULT '',
		output       TEXT NOT NULL DEFAULT '',
		error        TEXT NOT NULL DEFAULT '',
		retry_count  INTEGER NOT NULL DEFAULT 0,
		created_at   DATETIME NOT NULL,
		updated_at   DATETIME NOT NULL,
		started_at   DATETIME,
		completed_at DATETIME,
		FOREIGN KEY(agent_id) REFERENCES agents(id)
	);

	CREATE INDEX IF NOT EXISTS idx_tasks_status   ON tasks(status);
	CREATE INDEX IF NOT EXISTS idx_tasks_agent_id ON tasks(agent_id);

	CREATE TABLE IF NOT EXISTS checkpoints (
		id         TEXT PRIMARY KEY,
		task_id    TEXT NOT NULL,
		state      TEXT NOT NULL DEFAULT '{}',
		created_at DATETIME NOT NULL,
		FOREIGN KEY(task_id) REFERENCES tasks(id)
	);

	CREATE INDEX IF NOT EXISTS idx_checkpoints_task ON checkpoints(task_id);
	`
	_, err := s.db.Exec(schema)
	return err
}

// ─── Agent Registration ───────────────────────────────────────────────────────

func (s *Server) upsertBuiltinAgent(name, description string, capabilities []string) error {
	capJSON, _ := json.Marshal(capabilities)
	id := strings.ToLower(strings.ReplaceAll(name, " ", "-"))

	_, err := s.db.Exec(`
		INSERT INTO agents (id, name, description, capabilities, status, registered_at)
		VALUES (?, ?, ?, ?, 'active', ?)
		ON CONFLICT(id) DO UPDATE SET
			name         = excluded.name,
			description  = excluded.description,
			capabilities = excluded.capabilities,
			status       = 'active'
	`, id, name, description, string(capJSON), time.Now().UTC())
	if err != nil {
		return err
	}

	// Push to FDCL if configured
	if s.cfg.FDCLURL != "" {
		payload, _ := json.Marshal(map[string]interface{}{
			"agent_id":     id,
			"name":         name,
			"capabilities": capabilities,
		})
		go func() {
			req, _ := http.NewRequest("POST", s.cfg.FDCLURL+"/registry/agents", bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			http.DefaultClient.Do(req)
		}()
	}

	agentsRegistered.Inc()
	return nil
}

func (s *Server) registerBuiltins() error {
	if err := s.upsertBuiltinAgent(
		"InfraReportAgent",
		"Generates a plain-English infrastructure status report",
		[]string{"col:read", "ssi:read"},
	); err != nil {
		return fmt.Errorf("register InfraReportAgent: %w", err)
	}
	if err := s.upsertBuiltinAgent(
		"SearchAgent",
		"Searches the knowledge base for relevant information",
		[]string{"ssi:read"},
	); err != nil {
		return fmt.Errorf("register SearchAgent: %w", err)
	}
	// Phase 2: Anomaly Detection Agent
	if err := s.upsertBuiltinAgent(
		"AnomalyDetectionAgent",
		"Queries AICP to run an infrastructure anomaly diagnostic scan using AI analysis of Prometheus metrics and Loki logs",
		[]string{"aicp:diagnose", "col:read"},
	); err != nil {
		return fmt.Errorf("register AnomalyDetectionAgent: %w", err)
	}
	// Phase 3: New Agents
	if err := s.upsertBuiltinAgent(
		"MetricAnalysisAgent",
		"Analyzes Prometheus time-series metrics with AI to identify trends, rate services, and provide recommendations",
		[]string{"prometheus:read", "nvidia:chat"},
	); err != nil {
		return fmt.Errorf("register MetricAnalysisAgent: %w", err)
	}
	if err := s.upsertBuiltinAgent(
		"PredictiveScalingAgent",
		"AI-driven scaling agent — auto-executes low-risk actions, recommends high-risk infrastructure changes",
		[]string{"prometheus:read", "col:write", "nvidia:chat"},
	); err != nil {
		return fmt.Errorf("register PredictiveScalingAgent: %w", err)
	}
	if err := s.upsertBuiltinAgent(
		"KnowledgeSynthesisAgent",
		"Synthesizes knowledge from SSI search, episodic memory, and infra context into comprehensive documents",
		[]string{"ssi:read", "ssi:write", "nvidia:chat"},
	); err != nil {
		return fmt.Errorf("register KnowledgeSynthesisAgent: %w", err)
	}
	if err := s.upsertBuiltinAgent(
		"RemediationAgent",
		"Autonomous self-healing remediation agent that recovers services from critical anomalies",
		[]string{"col:write", "aicp:read"},
	); err != nil {
		return fmt.Errorf("register RemediationAgent: %w", err)
	}
	// Phase 5.5: Scheduler Agent
	if err := s.upsertBuiltinAgent(
		"SchedulerAgent",
		"Dynamically evaluates node telemetry across the mesh and deploys workloads to the optimal target node",
		[]string{"ikg:read", "col:deploy_mesh"},
	); err != nil {
		return fmt.Errorf("register SchedulerAgent: %w", err)
	}
	// Phase 5.6: Memory Compression Agent
	if err := s.upsertBuiltinAgent(
		"MemoryCompressionAgent",
		"Prunes and summarizes old infrastructure events and episodic memories to save space",
		[]string{"ssi:read", "ssi:write", "nvidia:chat"},
	); err != nil {
		return fmt.Errorf("register MemoryCompressionAgent: %w", err)
	}
	return nil
}

// ─── NATS ─────────────────────────────────────────────────────────────────────

func (s *Server) connectNATS() {
	var err error
	for attempt := 1; attempt <= 10; attempt++ {
		s.nc, err = nats.Connect(s.cfg.NATSURL,
			nats.Name("aaf-service"),
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(2*time.Second),
		)
		if err == nil {
			log.Printf("[NATS] connected to %s", s.cfg.NATSURL)
			break
		}
		log.Printf("[NATS] attempt %d failed: %v — retrying in 3s", attempt, err)
		time.Sleep(3 * time.Second)
	}
	if err != nil {
		log.Printf("[NATS] could not connect after retries — running without NATS: %v", err)
		return
	}

	_, subErr := s.nc.Subscribe("kalpana.aaf.tasks", func(msg *nats.Msg) {
		var req NATSTaskMessage
		if jsonErr := json.Unmarshal(msg.Data, &req); jsonErr != nil {
			log.Printf("[NATS] bad message: %v", jsonErr)
			return
		}
		taskID, submitErr := s.createTask(req.AgentID, req.Input, "")
		if submitErr != nil {
			log.Printf("[NATS] failed to create task for agent %s: %v", req.AgentID, submitErr)
			return
		}
		log.Printf("[NATS] dispatched task %s for agent %s", taskID, req.AgentID)
	})
	if subErr != nil {
		log.Printf("[NATS] subscribe error: %v", subErr)
	}
}

// ─── Task Helpers ─────────────────────────────────────────────────────────────

func generateID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

func (s *Server) createTask(agentID, input, token string) (string, error) {
	// Verify agent exists
	var status string
	var capsJSON string
	err := s.db.QueryRow("SELECT status, capabilities FROM agents WHERE id = ?", agentID).Scan(&status, &capsJSON)
	if err == sql.ErrNoRows {
		// Just log, we might route it to FDCL later
		log.Printf("[AAF] Agent %s not found locally, will try federation", agentID)
	} else if err != nil {
		return "", fmt.Errorf("agent lookup: %w", err)
	}

	id := generateID("task")
	now := time.Now().UTC()
	_, err = s.db.Exec(`
		INSERT INTO tasks (id, agent_id, status, input, output, error, retry_count, created_at, updated_at)
		VALUES (?, ?, 'PENDING', ?, '', '', 0, ?, ?)
	`, id, agentID, input, now, now)
	if err != nil {
		return "", fmt.Errorf("insert task: %w", err)
	}
	tasksTotal.WithLabelValues(agentID, "PENDING").Inc()

	// Launch async execution
	bgCtx := context.Background()
	if token != "" {
		bgCtx = context.WithValue(bgCtx, contextKeyToken, token)
	}
	ctx, cancel := context.WithTimeout(bgCtx, 10*time.Minute)
	s.cancelMu.Lock()
	s.cancels[id] = cancel
	s.cancelMu.Unlock()

	go func() {
		defer func() {
			cancel()
			s.cancelMu.Lock()
			delete(s.cancels, id)
			s.cancelMu.Unlock()
		}()
		s.executeTask(ctx, id, agentID, input)
	}()

	return id, nil
}

func (s *Server) updateTaskStatus(id, status, output, errMsg string, started, completed *time.Time) {
	now := time.Now().UTC()
	_, err := s.db.Exec(`
		UPDATE tasks SET
			status       = ?,
			output       = ?,
			error        = ?,
			updated_at   = ?,
			started_at   = COALESCE(started_at, ?),
			completed_at = ?
		WHERE id = ?
	`, status, output, errMsg, now, started, completed, id)
	if err != nil {
		log.Printf("[DB] updateTaskStatus %s: %v", id, err)
	}
}

func (s *Server) incrementRetry(id string) {
	_, err := s.db.Exec(`UPDATE tasks SET retry_count = retry_count + 1, updated_at = ? WHERE id = ?`,
		time.Now().UTC(), id)
	if err != nil {
		log.Printf("[DB] incrementRetry %s: %v", id, err)
	}
}

func (s *Server) writeCheckpoint(taskID, state string) {
	id := generateID("cp")
	_, err := s.db.Exec(`
		INSERT INTO checkpoints (id, task_id, state, created_at) VALUES (?, ?, ?, ?)
	`, id, taskID, state, time.Now().UTC())
	if err != nil {
		log.Printf("[DB] writeCheckpoint %s: %v", taskID, err)
	}
}

// ─── Task Execution ───────────────────────────────────────────────────────────

func (s *Server) executeTask(ctx context.Context, taskID, agentID, input string) {
	start := time.Now()
	now := start.UTC()
	s.updateTaskStatus(taskID, "RUNNING", "", "", &now, nil)
	tasksTotal.WithLabelValues(agentID, "RUNNING").Inc()

	// Start periodic checkpoint ticker
	ticker := time.NewTicker(30 * time.Second)
	checkpointDone := make(chan struct{})
	go func() {
		defer close(checkpointDone)
		for {
			select {
			case <-ticker.C:
				state, _ := json.Marshal(map[string]interface{}{
					"task_id":  taskID,
					"agent_id": agentID,
					"elapsed":  time.Since(start).String(),
					"status":   "RUNNING",
				})
				s.writeCheckpoint(taskID, string(state))
			case <-ctx.Done():
				return
			case <-checkpointDone:
				return
			}
		}
	}()

	defer func() {
		ticker.Stop()
	}()

	const maxRetries = 3
	var output string
	var execErr error

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("[AAF] retrying task %s (attempt %d/%d) after 2s", taskID, attempt, maxRetries)
			time.Sleep(2 * time.Second)
			s.incrementRetry(taskID)
		}

		select {
		case <-ctx.Done():
			ticker.Stop()
			completedAt := time.Now().UTC()
			s.updateTaskStatus(taskID, "FAILED", "", "task cancelled", &now, &completedAt)
			tasksTotal.WithLabelValues(agentID, "FAILED").Inc()
			taskDuration.WithLabelValues(agentID).Observe(time.Since(start).Seconds())
			return
		default:
		}

		output, execErr = s.dispatchAgent(ctx, agentID, input)
		if execErr == nil {
			break
		}
		log.Printf("[AAF] task %s attempt %d failed: %v", taskID, attempt+1, execErr)
	}

	ticker.Stop()
	// Wait for checkpoint goroutine to stop
	select {
	case <-checkpointDone:
	case <-time.After(1 * time.Second):
	}

	completedAt := time.Now().UTC()
	taskDuration.WithLabelValues(agentID).Observe(time.Since(start).Seconds())

	if execErr != nil {
		s.updateTaskStatus(taskID, "FAILED", "", execErr.Error(), &now, &completedAt)
		tasksTotal.WithLabelValues(agentID, "FAILED").Inc()

		// Write final failure checkpoint
		state, _ := json.Marshal(map[string]interface{}{
			"task_id": taskID,
			"status":  "FAILED",
			"error":   execErr.Error(),
		})
		s.writeCheckpoint(taskID, string(state))
		// Phase 2: write failure to episodic memory
		tokenStr, _ := ctx.Value(contextKeyToken).(string)
		writeTaskMemory(s.cfg.SSIURL, agentID, "FAILED", input, execErr.Error(), tokenStr)
	} else {
		s.updateTaskStatus(taskID, "COMPLETED", output, "", &now, &completedAt)
		tasksTotal.WithLabelValues(agentID, "COMPLETED").Inc()

		// Write final success checkpoint
		state, _ := json.Marshal(map[string]interface{}{
			"task_id": taskID,
			"status":  "COMPLETED",
		})
		s.writeCheckpoint(taskID, string(state))
		// Phase 2: write success to episodic memory
		tokenStr, _ := ctx.Value(contextKeyToken).(string)
		writeTaskMemory(s.cfg.SSIURL, agentID, "COMPLETED", input, output, tokenStr)
	}
}

func (s *Server) dispatchAgent(ctx context.Context, agentID, input string) (string, error) {
	switch agentID {
	case "infrareportagent":
		return s.runInfraReportAgent(ctx)
	case "searchagent":
		return s.runSearchAgent(ctx, input)
	case "anomalydetectionagent":
		return s.runAnomalyDetectionAgent(ctx, input)
	// Phase 3 agents
	case "metricanalysisagent":
		return s.runMetricAnalysisAgent(ctx, input)
	case "predictivescalingagent":
		return s.runPredictiveScalingAgent(ctx, input)
	case "knowledgesynthesisagent":
		return s.runKnowledgeSynthesisAgent(ctx, input)
	case "remediationagent", "RemediationAgent":
		return s.runRemediationAgent(ctx, input)
	case "scheduleragent", "SchedulerAgent":
		return s.runSchedulerAgent(ctx, input)
	case "memorycompressionagent", "MemoryCompressionAgent":
		return s.runMemoryCompressionAgent(ctx, input)
	default:
		// Try routing to FDCL if agent is not local
		if s.cfg.FDCLURL != "" {
			payload, _ := json.Marshal(map[string]string{
				"agent_id": agentID,
				"input":    input,
			})
			req, _ := http.NewRequestWithContext(ctx, "POST", s.cfg.FDCLURL+"/tasks/route", bytes.NewReader(payload))
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err == nil && resp.StatusCode == http.StatusOK {
				var result struct {
					Status string `json:"status"`
					Output string `json:"output"`
					Error  string `json:"error"`
				}
				json.NewDecoder(resp.Body).Decode(&result)
				resp.Body.Close()
				if result.Error != "" {
					return "", fmt.Errorf("federation error: %s", result.Error)
				}
				return result.Output, nil
			}
			if resp != nil {
				resp.Body.Close()
			}
		}
		return "", fmt.Errorf("no executor registered for agent %q locally or in federation", agentID)
	}
}

// ─── InfraReportAgent ─────────────────────────────────────────────────────────

func (s *Server) runInfraReportAgent(ctx context.Context) (string, error) {
	type colService struct {
		ID     string `json:"id"`
		Name   string `json:"name"`
		Status string `json:"status"`
		Image  string `json:"image"`
	}
	type colNodeResponse struct {
		Hostname      string `json:"hostname"`
		CPUCount      int    `json:"cpu_count"`
		TotalMemory   uint64 `json:"total_memory"`
		UsedMemory    uint64 `json:"used_memory"`
		GoVersion     string `json:"go_version"`
		DockerVersion string `json:"docker_version"`
		OS            string `json:"os"`
		Arch          string `json:"arch"`
	}
	type colAuditEntry struct {
		Action    string `json:"action"`
		Resource  string `json:"resource"`
		UserID    string `json:"user_id"`
		Timestamp string `json:"timestamp"`
	}

	// GET /services
	var services []colService
	if err := s.httpGet(ctx, s.cfg.COLURL+"/services", &services); err != nil {
		return "", fmt.Errorf("COL /services: %w", err)
	}

	// GET /nodes
	var node colNodeResponse
	if err := s.httpGet(ctx, s.cfg.COLURL+"/nodes", &node); err != nil {
		return "", fmt.Errorf("COL /nodes: %w", err)
	}

	// GET /audit
	type auditResponse struct {
		Entries []colAuditEntry `json:"entries"`
	}
	var auditResp auditResponse
	// audit might not exist; ignore error and continue
	_ = s.httpGet(ctx, s.cfg.COLURL+"/audit", &auditResp)

	// Build Markdown report
	var sb strings.Builder
	sb.WriteString("# KalpanaOS Infrastructure Status Report\n")
	sb.WriteString(fmt.Sprintf("**Generated:** %s\n\n", time.Now().UTC().Format(time.RFC1123)))

	// Services section
	sb.WriteString("## Services\n\n")
	if len(services) == 0 {
		sb.WriteString("_No services found._\n\n")
	} else {
		healthy := 0
		for _, svc := range services {
			if strings.EqualFold(svc.Status, "running") || strings.EqualFold(svc.Status, "healthy") {
				healthy++
			}
		}
		sb.WriteString(fmt.Sprintf("**Total:** %d | **Healthy:** %d | **Degraded:** %d\n\n",
			len(services), healthy, len(services)-healthy))
		sb.WriteString("| Name | Status | Image |\n|------|--------|-------|\n")
		for _, svc := range services {
			sb.WriteString(fmt.Sprintf("| %s | %s | %s |\n", svc.Name, svc.Status, svc.Image))
		}
		sb.WriteString("\n")
	}

	// Nodes section
	sb.WriteString("## System Host Node\n\n")
	sb.WriteString(fmt.Sprintf("- **Hostname:** %s\n", node.Hostname))
	sb.WriteString(fmt.Sprintf("- **OS / Arch:** %s / %s\n", node.OS, node.Arch))
	sb.WriteString(fmt.Sprintf("- **CPU Cores:** %d\n", node.CPUCount))
	if node.TotalMemory > 0 {
		sb.WriteString(fmt.Sprintf("- **Memory:** %.2f GB / %.2f GB (%.1f%% used)\n",
			float64(node.UsedMemory)/(1024*1024*1024),
			float64(node.TotalMemory)/(1024*1024*1024),
			float64(node.UsedMemory)/float64(node.TotalMemory)*100.0))
	} else {
		sb.WriteString("- **Memory:** unknown\n")
	}
	sb.WriteString(fmt.Sprintf("- **Docker Version:** %s\n", node.DockerVersion))
	sb.WriteString(fmt.Sprintf("- **Go Version:** %s\n\n", node.GoVersion))

	// Recent Audit Events
	sb.WriteString("## Recent Audit Events\n\n")
	if len(auditResp.Entries) == 0 {
		sb.WriteString("_No audit events available._\n\n")
	} else {
		limit := len(auditResp.Entries)
		if limit > 10 {
			limit = 10
		}
		sb.WriteString("| Time | User | Action | Resource |\n|------|------|--------|----------|\n")
		for _, e := range auditResp.Entries[:limit] {
			sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s |\n",
				e.Timestamp, e.UserID, e.Action, e.Resource))
		}
		sb.WriteString("\n")
	}

	sb.WriteString("---\n_Report generated by InfraReportAgent v1.0_\n")
	return sb.String(), nil
}

// ─── SearchAgent ──────────────────────────────────────────────────────────────

func (s *Server) runSearchAgent(ctx context.Context, query string) (string, error) {
	type searchRequest struct {
		Query string `json:"query"`
		Limit int    `json:"limit"`
	}
	type searchHit struct {
		ID       string  `json:"id"`
		Content  string  `json:"content"`
		Score    float64 `json:"score"`
		Source   string  `json:"source"`
	}
	type searchResponse struct {
		Hits  []searchHit `json:"hits"`
		Total int         `json:"total"`
	}

	reqBody, _ := json.Marshal(searchRequest{Query: query, Limit: 10})
	var result searchResponse
	if err := s.httpPost(ctx, s.cfg.SSIURL+"/search", reqBody, &result); err != nil {
		return "", fmt.Errorf("SSI /search: %w", err)
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("# Search Results for: %q\n\n", query))
	sb.WriteString(fmt.Sprintf("**Total Matches:** %d\n\n", result.Total))

	if len(result.Hits) == 0 {
		sb.WriteString("_No results found._\n")
		return sb.String(), nil
	}

	for i, hit := range result.Hits {
		sb.WriteString(fmt.Sprintf("### %d. %s\n", i+1, hit.ID))
		sb.WriteString(fmt.Sprintf("**Score:** %.4f | **Source:** %s\n\n", hit.Score, hit.Source))
		content := hit.Content
		if len(content) > 500 {
			content = content[:500] + "..."
		}
		sb.WriteString(content + "\n\n")
	}
	return sb.String(), nil
}

// ─── Phase 2: AnomalyDetectionAgent ────────────────────────────────────────────

func (s *Server) runAnomalyDetectionAgent(ctx context.Context, input string) (string, error) {
	type diagnoseResp struct {
		Report            string `json:"report"`
		AnomaliesDetected int    `json:"anomalies_detected"`
		ScannedAt         string `json:"scanned_at"`
	}

	reqBody, _ := json.Marshal(map[string]interface{}{"window_minutes": 30})
	diagnoseCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
	defer cancel()

	req, err := http.NewRequestWithContext(diagnoseCtx, http.MethodPost, s.cfg.AICPURL+"/diagnose",
		bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Sprintf("# Anomaly Detection Report\n\n**Error:** could not build request: %v", err), nil
	}
	req.Header.Set("Content-Type", "application/json")
	if token, ok := diagnoseCtx.Value(contextKeyToken).(string); ok && token != "" {
		req.Header.Set("Authorization", token)
	}

	client := &http.Client{Timeout: 3 * time.Minute}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Sprintf("# Anomaly Detection Report\n\n**Error:** AICP /diagnose unavailable: %v\n\nThe anomaly detection service requires AICP to be running and responding.", err), nil
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Sprintf("# Anomaly Detection Report\n\n**Error:** AICP returned %d: %s", resp.StatusCode, string(body)), nil
	}

	var result diagnoseResp
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "# Anomaly Detection Report\n\n**Error:** failed to decode AICP response", nil
	}

	var sb strings.Builder
	sb.WriteString("# Anomaly Detection Report\n\n")
	sb.WriteString(fmt.Sprintf("**Scan Time:** %s\n", result.ScannedAt))
	sb.WriteString(fmt.Sprintf("**Window:** 30 minutes\n"))
	sb.WriteString(fmt.Sprintf("**Anomalies Detected:** %d\n\n", result.AnomaliesDetected))
	sb.WriteString("## AI Analysis\n\n")
	sb.WriteString(result.Report)
	sb.WriteString("\n\n---\n_Report generated by AnomalyDetectionAgent v2.0_\n")
	return sb.String(), nil
}

// Phase 2: writeTaskMemory writes agent task outcomes to episodic memory (fire-and-forget).
func writeTaskMemory(ssiURL, agentID, status, input, output, token string) {
	go func() {
		summary := output
		if len(summary) > 500 {
			summary = summary[:500] + "..."
		}
		rec := map[string]interface{}{
			"type":    "agent_output",
			"source":  "aaf",
			"content": fmt.Sprintf("Agent %s %s: Input=%q Output=%s", agentID, status, input, summary),
			"tags":    []string{agentID, strings.ToLower(status)},
		}
		body, _ := json.Marshal(rec)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, ssiURL+"/memory", bytes.NewReader(body))
		if err != nil {
			log.Printf("[WARN] writeTaskMemory request build: %v", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", token)
		}
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			log.Printf("[WARN] writeTaskMemory send: %v", err)
			return
		}
		resp.Body.Close()
	}()
}

// ─── HTTP Helpers ─────────────────────────────────────────────────────────────

func (s *Server) httpGet(ctx context.Context, url string, dest interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if token, ok := ctx.Value(contextKeyToken).(string); ok && token != "" {
		req.Header.Set("Authorization", token)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return json.NewDecoder(resp.Body).Decode(dest)
}

func (s *Server) httpPost(ctx context.Context, url string, body []byte, dest interface{}) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if token, ok := ctx.Value(contextKeyToken).(string); ok && token != "" {
		req.Header.Set("Authorization", token)
	}
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
	}
	return json.NewDecoder(resp.Body).Decode(dest)
}

// ─── JWT Middleware ───────────────────────────────────────────────────────────

func (s *Server) jwtMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			respondJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing Authorization header"})
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			respondJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid Authorization format"})
			return
		}
		tokenStr := parts[1]

		// Remote validation via SIL
		if err := s.validateTokenRemote(r.Context(), tokenStr); err != nil {
			respondJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid token: " + err.Error()})
			return
		}

		// Also parse claims locally for context injection (best-effort)
		token, _, parseErr := new(jwt.Parser).ParseUnverified(tokenStr, jwt.MapClaims{})
		if parseErr == nil {
			if claims, ok := token.Claims.(jwt.MapClaims); ok {
				ctx := context.WithValue(r.Context(), contextKeyClaims{}, claims)
				ctx = context.WithValue(ctx, contextKeyToken, authHeader)
				r = r.WithContext(ctx)
			}
		}

		next.ServeHTTP(w, r)
	})
}

type contextKeyTokenStruct struct{}
var contextKeyToken = contextKeyTokenStruct{}

type contextKeyClaims struct{}

func (s *Server) validateTokenRemote(ctx context.Context, token string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.SILURL+"/validate", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		// If SIL is unavailable, fail open with a warning (dev mode)
		log.Printf("[JWT] SIL unavailable — failing open: %v", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("SIL returned %d: %s", resp.StatusCode, string(body))
	}

	var silResp struct {
		Valid bool   `json:"valid"`
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&silResp); err != nil {
		return fmt.Errorf("failed to decode SIL response: %w", err)
	}

	if !silResp.Valid {
		return fmt.Errorf("token invalid: %s", silResp.Error)
	}

	return nil
}

// ─── Response Helper ──────────────────────────────────────────────────────────

func respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "ok",
		"service": "aaf",
		"time":    time.Now().UTC(),
	})
}

// GET /agents
func (s *Server) handleListAgents(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(),
		"SELECT id, name, description, capabilities, status, registered_at FROM agents ORDER BY registered_at DESC")
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	agents := []Agent{}
	for rows.Next() {
		var a Agent
		var capJSON string
		var regAt string
		if err := rows.Scan(&a.ID, &a.Name, &a.Description, &capJSON, &a.Status, &regAt); err != nil {
			continue
		}
		_ = json.Unmarshal([]byte(capJSON), &a.Capabilities)
		a.RegisteredAt, _ = time.Parse(time.RFC3339Nano, regAt)
		agents = append(agents, a)
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{"agents": agents, "count": len(agents)})
}

// POST /agents
func (s *Server) handleRegisterAgent(w http.ResponseWriter, r *http.Request) {
	var req RegisterAgentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if strings.TrimSpace(req.Name) == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "name is required"})
		return
	}
	if strings.TrimSpace(req.Description) == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "description is required"})
		return
	}

	id := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(req.Name), " ", "-"))
	capJSON, _ := json.Marshal(req.Capabilities)

	_, err := s.db.ExecContext(r.Context(), `
		INSERT INTO agents (id, name, description, capabilities, status, registered_at)
		VALUES (?, ?, ?, ?, 'active', ?)
		ON CONFLICT(id) DO UPDATE SET
			description  = excluded.description,
			capabilities = excluded.capabilities,
			status       = 'active'
	`, id, req.Name, req.Description, string(capJSON), time.Now().UTC())
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	agentsRegistered.Inc()

	agent := Agent{
		ID:           id,
		Name:         req.Name,
		Description:  req.Description,
		Capabilities: req.Capabilities,
		Status:       "active",
		RegisteredAt: time.Now().UTC(),
	}
	respondJSON(w, http.StatusCreated, agent)
}

// GET /tasks
func (s *Server) handleListTasks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	statusFilter := q.Get("status")
	agentFilter := q.Get("agent_id")
	limitStr := q.Get("limit")
	limit := 50
	if limitStr != "" {
		if n, err := strconv.Atoi(limitStr); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}

	query := `SELECT id, agent_id, status, input, output, error, retry_count,
	           created_at, updated_at, started_at, completed_at
	           FROM tasks WHERE 1=1`
	args := []interface{}{}

	if statusFilter != "" {
		query += " AND status = ?"
		args = append(args, strings.ToUpper(statusFilter))
	}
	if agentFilter != "" {
		query += " AND agent_id = ?"
		args = append(args, agentFilter)
	}
	query += " ORDER BY created_at DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.db.QueryContext(r.Context(), query, args...)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	tasks := []Task{}
	for rows.Next() {
		t := scanTask(rows)
		if t != nil {
			tasks = append(tasks, *t)
		}
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{"tasks": tasks, "count": len(tasks)})
}

// POST /tasks
func (s *Server) handleSubmitTask(w http.ResponseWriter, r *http.Request) {
	var req SubmitTaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON: " + err.Error()})
		return
	}
	if strings.TrimSpace(req.AgentID) == "" {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "agent_id is required"})
		return
	}

	token := r.Header.Get("Authorization")
	taskID, err := s.createTask(req.AgentID, req.Input, token)
	if err != nil {
		if strings.Contains(err.Error(), "not found") {
			respondJSON(w, http.StatusNotFound, map[string]string{"error": err.Error()})
		} else {
			respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return
	}

	respondJSON(w, http.StatusAccepted, SubmitTaskResponse{
		TaskID: taskID,
		Status: "PENDING",
	})
}

// GET /tasks/{id}
func (s *Server) handleGetTask(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	row := s.db.QueryRowContext(r.Context(), `
		SELECT id, agent_id, status, input, output, error, retry_count,
		       created_at, updated_at, started_at, completed_at
		FROM tasks WHERE id = ?`, id)

	t := scanTaskRow(row)
	if t == nil {
		respondJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}
	respondJSON(w, http.StatusOK, t)
}

// DELETE /tasks/{id}
func (s *Server) handleCancelTask(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	// Check task exists and is running
	var status string
	err := s.db.QueryRowContext(r.Context(), "SELECT status FROM tasks WHERE id = ?", id).Scan(&status)
	if err == sql.ErrNoRows {
		respondJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if status != "PENDING" && status != "RUNNING" {
		respondJSON(w, http.StatusConflict, map[string]string{
			"error":  "task cannot be cancelled",
			"status": status,
		})
		return
	}

	// Cancel the goroutine
	s.cancelMu.Lock()
	if cancel, ok := s.cancels[id]; ok {
		cancel()
	}
	s.cancelMu.Unlock()

	// Mark as FAILED immediately in DB (goroutine may update again)
	completedAt := time.Now().UTC()
	now := completedAt
	s.updateTaskStatus(id, "FAILED", "", "cancelled by user", &now, &completedAt)

	respondJSON(w, http.StatusOK, map[string]string{"task_id": id, "status": "cancelled"})
}

// GET /tasks/{id}/checkpoints
func (s *Server) handleGetCheckpoints(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "id")

	// Verify task exists
	var count int
	if err := s.db.QueryRowContext(r.Context(), "SELECT COUNT(*) FROM tasks WHERE id = ?", taskID).Scan(&count); err != nil || count == 0 {
		respondJSON(w, http.StatusNotFound, map[string]string{"error": "task not found"})
		return
	}

	rows, err := s.db.QueryContext(r.Context(),
		"SELECT id, task_id, state, created_at FROM checkpoints WHERE task_id = ? ORDER BY created_at ASC", taskID)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	checkpoints := []Checkpoint{}
	for rows.Next() {
		var cp Checkpoint
		var createdAt string
		if err := rows.Scan(&cp.ID, &cp.TaskID, &cp.State, &createdAt); err != nil {
			continue
		}
		cp.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
		checkpoints = append(checkpoints, cp)
	}
	respondJSON(w, http.StatusOK, map[string]interface{}{"checkpoints": checkpoints, "count": len(checkpoints)})
}

// ─── Row Scanners ─────────────────────────────────────────────────────────────

type rowScanner interface {
	Scan(dest ...interface{}) error
}

func scanTask(rows *sql.Rows) *Task {
	return scanTaskRow(rows)
}

func scanTaskRow(row rowScanner) *Task {
	var t Task
	var startedAt, completedAt sql.NullString
	var createdAt, updatedAt string

	err := row.Scan(
		&t.ID, &t.AgentID, &t.Status, &t.Input, &t.Output, &t.Error, &t.RetryCount,
		&createdAt, &updatedAt, &startedAt, &completedAt,
	)
	if err != nil {
		return nil
	}

	t.CreatedAt, _ = time.Parse(time.RFC3339Nano, createdAt)
	t.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updatedAt)
	if startedAt.Valid && startedAt.String != "" {
		if ts, err := time.Parse(time.RFC3339Nano, startedAt.String); err == nil {
			t.StartedAt = &ts
		}
	}
	if completedAt.Valid && completedAt.String != "" {
		if ts, err := time.Parse(time.RFC3339Nano, completedAt.String); err == nil {
			t.CompletedAt = &ts
		}
	}
	return &t
}

// ─── Metrics Middleware ───────────────────────────────────────────────────────

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (sr *statusRecorder) WriteHeader(status int) {
	sr.status = status
	sr.ResponseWriter.WriteHeader(status)
}

func metricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		httpRequests.WithLabelValues(r.Method, r.URL.Path, strconv.Itoa(rec.status)).Inc()
	})
}

// ─── Router ───────────────────────────────────────────────────────────────────

func (s *Server) router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RealIP)
	r.Use(metricsMiddleware)

	// Public endpoints
	r.Get("/health", s.handleHealth)
	r.Handle("/metrics", promhttp.Handler())

	// Protected endpoints
	r.Group(func(r chi.Router) {
		r.Use(s.jwtMiddleware)

		r.Get("/agents", s.handleListAgents)
		r.Post("/agents", s.handleRegisterAgent)

		r.Get("/tasks", s.handleListTasks)
		r.Post("/tasks", s.handleSubmitTask)
		r.Get("/tasks/{id}", s.handleGetTask)
		r.Delete("/tasks/{id}", s.handleCancelTask)
		r.Get("/tasks/{id}/checkpoints", s.handleGetCheckpoints)
		// Phase 3: pipeline + fanout
		r.Post("/tasks/pipeline", s.handlePipeline)
		r.Post("/tasks/fanout", s.handleFanout)
	})

	return r
}

// ─── Main ─────────────────────────────────────────────────────────────────────

func main() {
	cfg := loadConfig()
	log.Printf("[AAF] starting — port=%s db=%s", cfg.Port, cfg.DBPath)

	srv, err := newServer(cfg)
	if err != nil {
		log.Fatalf("[AAF] init server: %v", err)
	}

	if err := srv.initDB(); err != nil {
		log.Fatalf("[AAF] init db: %v", err)
	}
	log.Println("[AAF] database initialised")

	if err := srv.registerBuiltins(); err != nil {
		log.Fatalf("[AAF] register builtins: %v", err)
	}
	log.Println("[AAF] built-in agents registered (Phase 1+2+3)")

	// Connect NATS in background (non-fatal)
	go srv.connectNATS()

	// Phase 3: heartbeat + orchestrator self-registration
	srv.startOrchestratorHeartbeat()

	httpSrv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      srv.router(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Minute,
		IdleTimeout:  60 * time.Second,
	}

	serve, mode := buildAAFTLSServer(httpSrv, cfg.CertFile, cfg.KeyFile, cfg.CAFile)
	log.Printf("[AAF] listening on :%s (%s)", cfg.Port, mode)
	if err := serve(); err != nil {
		log.Fatalf("[AAF] server error: %v", err)
	}
}

// ─── Phase 3: MetricAnalysisAgent ────────────────────────────────────────────

func (s *Server) runMetricAnalysisAgent(ctx context.Context, input string) (string, error) {
	now := time.Now().Unix()
	oneHour := now - 3600

	queryPrometheus := func(query string) string {
		url := fmt.Sprintf("%s/api/v1/query_range?query=%s&start=%d&end=%d&step=60",
			s.cfg.PrometheusURL, query, oneHour, now)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return ""
		}
		resp, err := s.httpClient.Do(req)
		if err != nil {
			return fmt.Sprintf("(prometheus unavailable: %v)", err)
		}
		defer resp.Body.Close()
		var pr struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
					Values [][]interface{}   `json:"values"`
				} `json:"result"`
			} `json:"data"`
		}
		json.NewDecoder(resp.Body).Decode(&pr)
		var sb strings.Builder
		for _, r := range pr.Data.Result {
			name := r.Metric["name"]
			if name == "" {
				name = r.Metric["container_name"]
			}
			if name == "" || len(r.Values) < 2 {
				continue
			}
			firstVal := fmt.Sprintf("%v", r.Values[0][1])
			lastVal := fmt.Sprintf("%v", r.Values[len(r.Values)-1][1])
			sb.WriteString(fmt.Sprintf("  %-40s start=%-12s end=%-12s samples=%d\n",
				name, firstVal, lastVal, len(r.Values)))
		}
		return sb.String()
	}

	memData := queryPrometheus("container_memory_usage_bytes%7Bname%3D~%22kalpana-.*%22%7D")
	cpuData := queryPrometheus("rate(container_cpu_usage_seconds_total%7Bname%3D~%22kalpana-.*%22%7D%5B5m%5D)")

	promCtx := fmt.Sprintf("Memory Usage (bytes, 1h trend):\n%s\n\nCPU Usage Rate (5m avg, 1h trend):\n%s", memData, cpuData)
	if memData == "" && cpuData == "" {
		promCtx = "Prometheus data unavailable. Provide general best-practice recommendations."
	}

	systemPrompt := `You are a Prometheus metrics analyst for KalpanaOS. Analyze the given time-series data and:
1. Identify which services consume the most resources
2. Detect concerning trends (memory growth, CPU spikes)
3. Rate each service: HEALTHY | DEGRADED | CRITICAL
4. Provide specific, actionable recommendations
Format your response as structured Markdown with headers, bullet points, and a summary table.`

	analysis, err := s.callNVIDIA(ctx, systemPrompt,
		fmt.Sprintf("Analyze this KalpanaOS Prometheus data:\n\n%s\n\nAdditional context: %s", promCtx, input))
	if err != nil {
		return fmt.Sprintf("# Metric Analysis Report\n\n**NVIDIA API Error:** %v\n\n## Raw Prometheus Data\n\n```\n%s\n```", err, promCtx), nil
	}
	return fmt.Sprintf("# Metric Analysis Report\n\n_Generated by MetricAnalysisAgent v3.0 — %s_\n\n%s",
		time.Now().UTC().Format(time.RFC1123), analysis), nil
}

// ─── Phase 3: PredictiveScalingAgent ─────────────────────────────────────────

func (s *Server) runPredictiveScalingAgent(ctx context.Context, input string) (string, error) {
	metricsReport, _ := s.runMetricAnalysisAgent(ctx, input)

	systemPrompt := `You are an infrastructure scaling expert for KalpanaOS. Based on the metrics analysis below, identify scaling actions needed.

For each action, output EXACTLY this format (one per line):
ACTION: risk=<LOW|HIGH> type=<restart|memory_bump|alert|deploy|stop> service=<service-name> detail=<one sentence>

LOW risk: restarting crashed/unresponsive services, sending alerts, minor memory adjustments
HIGH risk: deploying new services, stopping existing services, major config changes

If no action is needed: output NO_ACTION

After the ACTION lines, write a brief Markdown analysis summary.`

	llmResponse, err := s.callNVIDIA(ctx, systemPrompt,
		fmt.Sprintf("Metrics Analysis:\n%s\n\nContext: %s", metricsReport, input))
	if err != nil {
		return fmt.Sprintf("# Predictive Scaling Report\n\n**Error:** %v", err), nil
	}

	var executed, recommended, analysisLines []string

	for _, line := range strings.Split(llmResponse, "\n") {
		if !strings.HasPrefix(line, "ACTION:") {
			analysisLines = append(analysisLines, line)
			continue
		}
		parts := strings.TrimPrefix(line, "ACTION:")
		var risk, actionType, service, detail string
		for _, kv := range strings.Fields(parts) {
			switch {
			case strings.HasPrefix(kv, "risk="):
				risk = strings.TrimPrefix(kv, "risk=")
			case strings.HasPrefix(kv, "type="):
				actionType = strings.TrimPrefix(kv, "type=")
			case strings.HasPrefix(kv, "service="):
				service = strings.TrimPrefix(kv, "service=")
			case strings.HasPrefix(kv, "detail="):
				detail = strings.TrimPrefix(kv, "detail=")
			}
		}

		if strings.ToUpper(risk) == "LOW" {
			if s.nc != nil && strings.ToLower(actionType) == "alert" {
				alertPayload, _ := json.Marshal(map[string]string{
					"type": "scaling_alert", "service": service, "detail": detail,
				})
				s.nc.Publish("kalpana.alerts", alertPayload)
			}
			executed = append(executed, fmt.Sprintf("✅ %s on %s — %s", actionType, service, detail))
		} else {
			recommended = append(recommended, fmt.Sprintf("🔴 HIGH-RISK %s on %s — %s", actionType, service, detail))
		}
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("# Predictive Scaling Report\n\n_Generated %s_\n\n", time.Now().UTC().Format(time.RFC1123)))
	if len(executed) > 0 {
		sb.WriteString("## ✅ Auto-Executed Actions (LOW Risk)\n\n")
		for _, a := range executed {
			sb.WriteString("- " + a + "\n")
		}
		sb.WriteString("\n")
	}
	if len(recommended) > 0 {
		sb.WriteString("## ⚠️ Recommended Actions (HIGH Risk — Awaiting Approval)\n\n")
		for _, a := range recommended {
			sb.WriteString("- " + a + "\n")
		}
		sb.WriteString("\n")
	}
	if len(executed) == 0 && len(recommended) == 0 {
		sb.WriteString("## No Scaling Actions Required\n\nInfrastructure appears healthy.\n\n")
	}
	sb.WriteString("## AI Analysis\n\n" + strings.Join(analysisLines, "\n"))
	sb.WriteString("\n\n---\n_Report by PredictiveScalingAgent v3.0_\n")
	return sb.String(), nil
}

// ─── Phase 3: KnowledgeSynthesisAgent ────────────────────────────────────────

func aafMin(n, max int) int {
	if n > max {
		return max
	}
	return n
}

func (s *Server) runKnowledgeSynthesisAgent(ctx context.Context, topic string) (string, error) {
	if topic == "" {
		topic = "KalpanaOS infrastructure"
	}

	type searchResult struct {
		Text   string  `json:"text"`
		Source string  `json:"source"`
		Score  float64 `json:"score"`
	}
	type searchResp struct {
		Results []searchResult `json:"results"`
	}

	var ssiResults, memResults searchResp
	ssiBody, _ := json.Marshal(map[string]interface{}{"query": topic, "top_k": 5})
	_ = s.httpPost(ctx, s.cfg.SSIURL+"/search", ssiBody, &ssiResults)
	memBody, _ := json.Marshal(map[string]interface{}{"query": topic, "limit": 5})
	_ = s.httpPost(ctx, s.cfg.SSIURL+"/memory/search", memBody, &memResults)

	type colService struct {
		Name   string `json:"name"`
		Status string `json:"status"`
		Image  string `json:"image"`
	}
	var services []colService
	_ = s.httpGet(ctx, s.cfg.COLURL+"/services", &services)

	var ctxBuilder strings.Builder
	ctxBuilder.WriteString("## Knowledge Base Search Results\n")
	for i, r := range ssiResults.Results {
		text := r.Text
		if len(text) > 500 {
			text = text[:500] + "..."
		}
		ctxBuilder.WriteString(fmt.Sprintf("%d. [%s] %s\n", i+1, r.Source, text))
	}
	ctxBuilder.WriteString("\n## Episodic Memory\n")
	for i, r := range memResults.Results {
		text := r.Text
		if len(text) > 300 {
			text = text[:300] + "..."
		}
		ctxBuilder.WriteString(fmt.Sprintf("%d. %s\n", i+1, text))
	}
	ctxBuilder.WriteString("\n## Current Infrastructure\n")
	for _, svc := range services {
		ctxBuilder.WriteString(fmt.Sprintf("- %s (%s): %s\n", svc.Name, svc.Image, svc.Status))
	}

	doc, err := s.callNVIDIA(ctx,
		`You are a technical documentation specialist for KalpanaOS. Synthesize the provided context into a comprehensive, well-structured technical document about the given topic.
Include: Overview, Current State, Key Insights, Recommendations, Related Resources.
Use Markdown formatting with clear headers, bullet points, and code blocks where appropriate.`,
		fmt.Sprintf("Topic: %s\n\nContext:\n%s", topic, ctxBuilder.String()))
	if err != nil {
		return fmt.Sprintf("# Knowledge Synthesis: %s\n\n**Error:** %v\n\n## Available Context\n\n%s",
			topic, err, ctxBuilder.String()), nil
	}

	// Ingest synthesized doc back into SSI knowledge base
	ingestBody, _ := json.Marshal(map[string]string{
		"text":   doc,
		"source": fmt.Sprintf("synthesis-%d", time.Now().Unix()),
	})
	_ = s.httpPost(ctx, s.cfg.SSIURL+"/ingest/text", ingestBody, nil)

	return fmt.Sprintf("# Knowledge Synthesis: %s\n\n_Synthesized %s | Auto-indexed to knowledge base_\n\n%s",
		topic, time.Now().UTC().Format(time.RFC1123), doc), nil
}

// ─── Phase 3: Pipeline Handler ────────────────────────────────────────────────

type PipelineStep struct {
	AgentID string `json:"agent_id"`
	Input   string `json:"input"`
}

type PipelineRequest struct {
	Pipeline []PipelineStep `json:"pipeline"`
}

func (s *Server) handlePipeline(w http.ResponseWriter, r *http.Request) {
	var req PipelineRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Pipeline) == 0 {
		http.Error(w, `{"error":"pipeline array required"}`, http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Minute)
	defer cancel()

	type StepResult struct {
		Step    int    `json:"step"`
		AgentID string `json:"agent_id"`
		Output  string `json:"output"`
		Error   string `json:"error,omitempty"`
	}

	results := make([]StepResult, 0, len(req.Pipeline))
	prevOutputs := map[int]string{}

	for i, step := range req.Pipeline {
		input := step.Input
		for n, out := range prevOutputs {
			input = strings.ReplaceAll(input, fmt.Sprintf("{{step_%d_output}}", n), out)
		}
		output, err := s.dispatchAgent(ctx, step.AgentID, input)
		sr := StepResult{Step: i, AgentID: step.AgentID, Output: output}
		if err != nil {
			sr.Error = err.Error()
		}
		results = append(results, sr)
		prevOutputs[i] = output
	}

	finalOutput := ""
	if len(results) > 0 {
		finalOutput = results[len(results)-1].Output
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"steps": results, "final_output": finalOutput, "step_count": len(results),
	})
}

// ─── Phase 3: Fanout Handler ──────────────────────────────────────────────────

type FanoutTask struct {
	AgentID string `json:"agent_id"`
	Input   string `json:"input"`
}

type FanoutRequest struct {
	Tasks         []FanoutTask `json:"tasks"`
	MergeStrategy string       `json:"merge_strategy"` // "concat" or "llm_merge"
}

func (s *Server) handleFanout(w http.ResponseWriter, r *http.Request) {
	var req FanoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.Tasks) == 0 {
		http.Error(w, `{"error":"tasks array required"}`, http.StatusBadRequest)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Minute)
	defer cancel()

	type TaskResult struct {
		AgentID string `json:"agent_id"`
		Output  string `json:"output"`
		Error   string `json:"error,omitempty"`
	}

	results := make([]TaskResult, len(req.Tasks))
	var wg sync.WaitGroup
	for i, task := range req.Tasks {
		wg.Add(1)
		go func(idx int, t FanoutTask) {
			defer wg.Done()
			output, err := s.dispatchAgent(ctx, t.AgentID, t.Input)
			results[idx] = TaskResult{AgentID: t.AgentID, Output: output}
			if err != nil {
				results[idx].Error = err.Error()
			}
		}(i, task)
	}
	wg.Wait()

	var merged string
	if req.MergeStrategy == "llm_merge" {
		var sb strings.Builder
		for _, res := range results {
			if res.Output != "" {
				sb.WriteString(fmt.Sprintf("=== %s ===\n%s\n\n", res.AgentID, res.Output))
			}
		}
		llmCtx, llmCancel := context.WithTimeout(ctx, 2*time.Minute)
		defer llmCancel()
		synthesized, err := s.callNVIDIA(llmCtx,
			"You are a KalpanaOS report synthesizer. Merge these parallel agent outputs into one coherent, well-structured Markdown report. Eliminate duplicates, highlight agreements, and note conflicts.",
			sb.String())
		if err != nil {
			merged = sb.String()
		} else {
			merged = synthesized
		}
	} else {
		var sb strings.Builder
		for _, res := range results {
			if res.Output != "" {
				sb.WriteString(fmt.Sprintf("## %s\n\n%s\n\n---\n\n", res.AgentID, res.Output))
			}
		}
		merged = sb.String()
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"results": results, "merged_output": merged, "task_count": len(results),
	})
}

// ─── Phase 3: Orchestrator Heartbeat + Registration ──────────────────────────

func (s *Server) startOrchestratorHeartbeat() {
	go func() {
		time.Sleep(5 * time.Second)
		s.registerWithOrchestrator()
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			s.publishHeartbeat()
		}
	}()
}

func (s *Server) registerWithOrchestrator() {
	agents := []string{
		"infrareportagent", "searchagent", "anomalydetectionagent",
		"metricanalysisagent", "predictivescalingagent", "knowledgesynthesisagent",
	}
	for _, agentID := range agents {
		var name, desc, caps string
		s.db.QueryRow("SELECT name, description, capabilities FROM agents WHERE id=?", agentID).Scan(&name, &desc, &caps)
		var capList []string
		json.Unmarshal([]byte(caps), &capList)

		body, _ := json.Marshal(map[string]interface{}{
			"id": agentID, "name": name, "description": desc,
			"capabilities": capList, "max_concurrency": 5,
		})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		req, _ := http.NewRequestWithContext(ctx, "POST",
			s.cfg.ORCHESTRATORURL+"/agents/register", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		resp, err := s.httpClient.Do(req)
		cancel()
		if err != nil {
			log.Printf("[Orchestrator] registration failed for %s: %v", agentID, err)
			continue
		}
		resp.Body.Close()
	}
	log.Printf("[Orchestrator] registered 6 agents with orchestrator service")
}

func (s *Server) publishHeartbeat() {
	if s.nc == nil || !s.nc.IsConnected() {
		return
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"agent_source": "aaf",
		"agents": []string{
			"infrareportagent", "searchagent", "anomalydetectionagent",
			"metricanalysisagent", "predictivescalingagent", "knowledgesynthesisagent",
		},
		"timestamp": time.Now(),
	})
	s.nc.Publish("kalpana.agent.heartbeat", payload)
}

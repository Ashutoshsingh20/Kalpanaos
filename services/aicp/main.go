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
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/golang-jwt/jwt/v5"
	nats "github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

type Config struct {
	Port             string
	NvidiaAPIKey     string
	NvidiaAPIBase    string
	NvidiaChatModel  string
	SSIURL           string
	COLURL           string
	AAFURL           string
	SILURL           string
	NATSURL          string
	StatePath        string
	// Phase 2
	PrometheusURL    string
	LokiURL          string
	// Phase 3
	ORCHESTRATORURL  string
	// Phase 4 - Edge Fallback
	OllamaURL        string
	OllamaModel      string
}

func loadConfig() Config {
	return Config{
		Port:            envOr("PORT", "8004"),
		NvidiaAPIKey:    envOr("NVIDIA_API_KEY", "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"),
		NvidiaAPIBase:   envOr("NVIDIA_API_BASE_URL", "https://integrate.api.nvidia.com/v1"),
		NvidiaChatModel: envOr("NVIDIA_CHAT_MODEL", "meta/llama-3.1-8b-instruct"),
		SSIURL:          envOr("SSI_URL", "http://ssi:8003"),
		COLURL:          envOr("COL_URL", "http://col:8002"),
		AAFURL:          envOr("AAF_URL", "http://aaf:8005"),
		SILURL:          envOr("SIL_URL", "http://sil:8001"),
		NATSURL:         envOr("NATS_URL", nats.DefaultURL),
		StatePath:       envOr("STATE_PATH", "/data/aicp_state.json"),
		PrometheusURL:   envOr("PROMETHEUS_URL", "http://prometheus:9090"),
		LokiURL:         envOr("LOKI_URL", "http://loki:3100"),
		ORCHESTRATORURL: envOr("ORCHESTRATOR_URL", "http://orchestrator:8006"),
		OllamaURL:       envOr("OLLAMA_URL", "http://host.docker.internal:11434"),
		OllamaModel:     envOr("OLLAMA_MODEL", "llama3.2"),
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// ---------------------------------------------------------------------------
// Domain models
// ---------------------------------------------------------------------------

type Message struct {
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	Timestamp time.Time `json:"timestamp"`
}

type ChatSession struct {
	ID        string    `json:"id"`
	Messages  []Message `json:"messages"`
	CreatedAt time.Time `json:"created_at"`
}

type PendingCommand struct {
	ID                  string          `json:"id"`
	SessionID           string          `json:"session_id"`
	CommandType         string          `json:"command_type"`
	Payload             json.RawMessage `json:"payload"`
	Description         string          `json:"description"`
	RequiresConfirmation bool           `json:"requires_confirmation"`
	CreatedAt           time.Time       `json:"created_at"`
}

// Phase 2: AnomalyRecord represents a detected infrastructure anomaly.
type AnomalyRecord struct {
	ID               string     `json:"id"`
	Severity         string     `json:"severity"`   // LOW | MEDIUM | HIGH | CRITICAL
	Title            string     `json:"title"`
	Description      string     `json:"description"`
	ServicesAffected []string   `json:"services_affected"`
	DetectedAt       time.Time  `json:"detected_at"`
	Resolved         bool       `json:"resolved"`
	ResolvedAt       *time.Time `json:"resolved_at,omitempty"`
	ResolvedNote     string     `json:"resolved_note,omitempty"`
}

type State struct {
	Sessions        map[string]*ChatSession    `json:"sessions"`
	PendingCommands map[string]*PendingCommand `json:"pending_commands"`
	Anomalies       map[string]*AnomalyRecord  `json:"anomalies"`
}

// ---------------------------------------------------------------------------
// Prometheus metrics
// ---------------------------------------------------------------------------

var (
	httpRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aicp_http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "path", "status"},
	)
	httpRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "aicp_http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)
	nvidiaRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "aicp_nvidia_requests_total",
			Help: "Total number of NVIDIA API requests",
		},
		[]string{"status"},
	)
	nvidiaRequestDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "aicp_nvidia_request_duration_seconds",
			Help:    "NVIDIA API request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)
	activeSessions = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "aicp_active_sessions",
			Help: "Number of active chat sessions",
		},
	)
	pendingCommandsGauge = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "aicp_pending_commands",
			Help: "Number of pending commands awaiting confirmation",
		},
	)
)

func init() {
	prometheus.MustRegister(
		httpRequestsTotal,
		httpRequestDuration,
		nvidiaRequestsTotal,
		nvidiaRequestDuration,
		activeSessions,
		pendingCommandsGauge,
	)
}

// ---------------------------------------------------------------------------
// State Store
// ---------------------------------------------------------------------------

type Store struct {
	mu        sync.RWMutex
	state     State
	statePath string
}

func NewStore(path string) (*Store, error) {
	s := &Store{
		statePath: path,
		state: State{
			Sessions:        make(map[string]*ChatSession),
			PendingCommands: make(map[string]*PendingCommand),
			Anomalies:       make(map[string]*AnomalyRecord),
		},
	}
	if err := s.load(); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("loading state: %w", err)
	}
	// Ensure anomalies map is initialised after load
	if s.state.Anomalies == nil {
		s.state.Anomalies = make(map[string]*AnomalyRecord)
	}
	return s, nil
}

func (s *Store) load() error {
	data, err := os.ReadFile(s.statePath)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Unmarshal(data, &s.state)
}

func (s *Store) save() error {
	s.mu.RLock()
	data, err := json.Marshal(s.state)
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.statePath)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return err
	}
	tmp := s.statePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0640); err != nil {
		return err
	}
	return os.Rename(tmp, s.statePath)
}

func (s *Store) GetSession(id string) (*ChatSession, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.state.Sessions[id]
	return sess, ok
}

func (s *Store) UpsertSession(sess *ChatSession) {
	s.mu.Lock()
	s.state.Sessions[sess.ID] = sess
	s.mu.Unlock()
	activeSessions.Set(float64(s.sessionCount()))
}

func (s *Store) DeleteSession(id string) {
	s.mu.Lock()
	delete(s.state.Sessions, id)
	s.mu.Unlock()
	activeSessions.Set(float64(s.sessionCount()))
}

func (s *Store) ListSessions() []*ChatSession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*ChatSession, 0, len(s.state.Sessions))
	for _, v := range s.state.Sessions {
		out = append(out, v)
	}
	return out
}

func (s *Store) sessionCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.state.Sessions)
}

func (s *Store) AddPending(cmd *PendingCommand) {
	s.mu.Lock()
	s.state.PendingCommands[cmd.ID] = cmd
	s.mu.Unlock()
	pendingCommandsGauge.Set(float64(s.pendingCount()))
}

func (s *Store) GetPending(id string) (*PendingCommand, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cmd, ok := s.state.PendingCommands[id]
	return cmd, ok
}

func (s *Store) RemovePending(id string) {
	s.mu.Lock()
	delete(s.state.PendingCommands, id)
	s.mu.Unlock()
	pendingCommandsGauge.Set(float64(s.pendingCount()))
}

func (s *Store) ListPending() []*PendingCommand {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*PendingCommand, 0, len(s.state.PendingCommands))
	for _, v := range s.state.PendingCommands {
		out = append(out, v)
	}
	return out
}

func (s *Store) pendingCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.state.PendingCommands)
}

// Phase 2: Anomaly store methods.
func (s *Store) AddAnomaly(a *AnomalyRecord) {
	s.mu.Lock()
	s.state.Anomalies[a.ID] = a
	s.mu.Unlock()
}

func (s *Store) GetAnomaly(id string) (*AnomalyRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.state.Anomalies[id]
	return a, ok
}

func (s *Store) ListAnomalies(resolvedOnly *bool) []*AnomalyRecord {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*AnomalyRecord, 0, len(s.state.Anomalies))
	for _, a := range s.state.Anomalies {
		if resolvedOnly != nil && a.Resolved != *resolvedOnly {
			continue
		}
		out = append(out, a)
	}
	return out
}

func (s *Store) ResolveAnomaly(id, note string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.state.Anomalies[id]
	if !ok {
		return false
	}
	now := time.Now().UTC()
	a.Resolved = true
	a.ResolvedAt = &now
	a.ResolvedNote = note
	return true
}

// ---------------------------------------------------------------------------
// NVIDIA API client
// ---------------------------------------------------------------------------

const systemPrompt = `You are KalpanaAI, the intelligent control plane of KalpanaOS - a sovereign AI-native cloud operating system. You help operators manage their infrastructure through natural language. You have access to infrastructure state and can issue commands. For destructive operations (stopping or deleting services), always ask for confirmation first. Be concise, technical, and precise.

When you recommend an infrastructure action, format it as: [ACTION: command_type | description]

Available commands and their formats:
1. Deploying a Git repository as an application (RCE & ADEL pipeline):
   [ACTION: deploy_app | name=<app_name> repo=<git_repo_url> platform=<web|android|ios>]
   Always infer the application 'name' (lowercase, alphanumeric, no spaces, e.g. 'smartevent' from 'SmartEvent-AI.git') and determine the platform (default is 'web'). This command will run immediately.
2. Deploying a generic Docker container service:
   [ACTION: deploy_service | name=<service_name> image=<docker_image> ports=<ports_comma_separated> env=<env_comma_separated>]
3. Stopping/removing a service:
   [ACTION: remove_service | name=<service_name>]
4. Restarting a service:
   [ACTION: restart_service | name=<service_name>]
5. Scaling a service:
   [ACTION: scale_service | name=<service_name> replicas=<number_of_replicas>]
6. Rolling back a service:
   [ACTION: rollback_service | name=<service_name>]`

type NvidiaMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type NvidiaRequest struct {
	Model       string          `json:"model"`
	Messages    []NvidiaMessage `json:"messages"`
	Temperature float64         `json:"temperature"`
	MaxTokens   int             `json:"max_tokens"`
	Stream      bool            `json:"stream"`
}

type NvidiaChoice struct {
	Message NvidiaMessage `json:"message"`
}

type NvidiaResponse struct {
	Choices []NvidiaChoice `json:"choices"`
}

type NvidiaClient struct {
	apiKey      string
	baseURL     string
	model       string
	ollamaURL   string
	ollamaModel string
	hc          *http.Client
}

func NewNvidiaClient(apiKey, baseURL, model, ollamaURL, ollamaModel string) *NvidiaClient {
	return &NvidiaClient{
		apiKey:      apiKey,
		baseURL:     strings.TrimRight(baseURL, "/"),
		model:       model,
		ollamaURL:   strings.TrimRight(ollamaURL, "/"),
		ollamaModel: ollamaModel,
		hc:          &http.Client{Timeout: 90 * time.Second},
	}
}

func (n *NvidiaClient) Chat(ctx context.Context, messages []NvidiaMessage) (string, error) {
	payload := NvidiaRequest{
		Model:       n.model,
		Messages:    messages,
		Temperature: 0.7,
		MaxTokens:   1024,
		Stream:      false,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal nvidia request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create nvidia request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+n.apiKey)

	start := time.Now()
	resp, err := n.hc.Do(req)
	elapsed := time.Since(start).Seconds()
	nvidiaRequestDuration.Observe(elapsed)

	if err != nil {
		nvidiaRequestsTotal.WithLabelValues("error").Inc()
		if n.ollamaURL != "" {
			log.Printf("[edge] Nvidia API error: %v — falling back to local Ollama", err)
			return n.chatOllama(ctx, messages)
		}
		return "", fmt.Errorf("nvidia api request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		nvidiaRequestsTotal.WithLabelValues("error").Inc()
		b, _ := io.ReadAll(resp.Body)
		if n.ollamaURL != "" {
			log.Printf("[edge] Nvidia API status %d: %s — falling back to local Ollama", resp.StatusCode, string(b))
			return n.chatOllama(ctx, messages)
		}
		return "", fmt.Errorf("nvidia api returned %d: %s", resp.StatusCode, string(b))
	}

	var nresp NvidiaResponse
	if err := json.NewDecoder(resp.Body).Decode(&nresp); err != nil {
		nvidiaRequestsTotal.WithLabelValues("error").Inc()
		return "", fmt.Errorf("decode nvidia response: %w", err)
	}
	if len(nresp.Choices) == 0 {
		nvidiaRequestsTotal.WithLabelValues("error").Inc()
		return "", errors.New("nvidia api returned empty choices")
	}
	nvidiaRequestsTotal.WithLabelValues("ok").Inc()
	return nresp.Choices[0].Message.Content, nil
}

func (n *NvidiaClient) chatOllama(ctx context.Context, messages []NvidiaMessage) (string, error) {
	payload := NvidiaRequest{
		Model:       n.ollamaModel,
		Messages:    messages,
		Temperature: 0.7,
		MaxTokens:   1024,
		Stream:      false,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal ollama request: %w", err)
	}

	url := n.ollamaURL + "/v1/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("create ollama request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.hc.Do(req)
	if err != nil {
		log.Printf("[edge] Ollama OpenAI endpoint failed: %v. Trying native /api/chat...", err)
		return n.chatOllamaNative(ctx, messages)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		log.Printf("[edge] Ollama OpenAI endpoint status %d: %s. Trying native /api/chat...", resp.StatusCode, string(b))
		return n.chatOllamaNative(ctx, messages)
	}

	var nresp NvidiaResponse
	if err := json.NewDecoder(resp.Body).Decode(&nresp); err != nil || len(nresp.Choices) == 0 {
		return n.chatOllamaNative(ctx, messages)
	}
	nvidiaRequestsTotal.WithLabelValues("ok").Inc()
	return nresp.Choices[0].Message.Content, nil
}

func (n *NvidiaClient) chatOllamaNative(ctx context.Context, messages []NvidiaMessage) (string, error) {
	type OllamaMsg struct {
		Role    string   `json:"role"`
		Content string   `json:"content"`
	}
	type OllamaReq struct {
		Model    string      `json:"model"`
		Messages []OllamaMsg `json:"messages"`
		Stream   bool        `json:"stream"`
	}
	var omsgs []OllamaMsg
	for _, m := range messages {
		omsgs = append(omsgs, OllamaMsg{Role: m.Role, Content: m.Content})
	}
	payload := OllamaReq{
		Model:    n.ollamaModel,
		Messages: omsgs,
		Stream:   false,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return "", err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.ollamaURL+"/api/chat", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := n.hc.Do(req)
	if err != nil {
		nvidiaRequestsTotal.WithLabelValues("error").Inc()
		return "", fmt.Errorf("ollama native API failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		nvidiaRequestsTotal.WithLabelValues("error").Inc()
		b, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("ollama native API status %d: %s", resp.StatusCode, string(b))
	}

	var oresp struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&oresp); err != nil {
		nvidiaRequestsTotal.WithLabelValues("error").Inc()
		return "", fmt.Errorf("decode ollama native response: %w", err)
	}

	nvidiaRequestsTotal.WithLabelValues("ok").Inc()
	return oresp.Message.Content, nil
}

// Ping checks connectivity to the NVIDIA API.
func (n *NvidiaClient) Ping(ctx context.Context) error {
	testMsg := []NvidiaMessage{
		{Role: "user", Content: "ping"},
	}
	payload := NvidiaRequest{
		Model:       n.model,
		Messages:    testMsg,
		Temperature: 0,
		MaxTokens:   1,
		Stream:      false,
	}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+n.apiKey)
	resp, err := n.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// ---------------------------------------------------------------------------
// RAG helpers — SSI & COL
// ---------------------------------------------------------------------------

type SSISearchResult struct {
	ID      string  `json:"id"`
	Content string  `json:"content"`
	Score   float64 `json:"score"`
}

func ssiSearch(ctx context.Context, baseURL, query, token string) ([]SSISearchResult, error) {
	payload := map[string]interface{}{"query": query, "top_k": 3}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, baseURL+"/search", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ssi returned %d", resp.StatusCode)
	}
	var result struct {
		Results []struct {
			Text   string  `json:"text"`
			Source string  `json:"source"`
			Score  float64 `json:"score"`
		} `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	out := make([]SSISearchResult, 0, len(result.Results))
	for _, r := range result.Results {
		out = append(out, SSISearchResult{Content: r.Text, Score: r.Score})
	}
	return out, nil
}

// Phase 2: search episodic memory.
type MemorySearchResult struct {
	Content   string `json:"content"`
	Type      string `json:"type"`
	Source    string `json:"source"`
	CreatedAt string `json:"created_at"`
}

func ssiMemorySearch(ctx context.Context, ssiURL, query, token string) ([]MemorySearchResult, error) {
	payload := map[string]interface{}{"query": query, "limit": 3}
	body, _ := json.Marshal(payload)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ssiURL+"/memory/search", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ssi memory search returned %d", resp.StatusCode)
	}
	var result struct {
		Results []MemorySearchResult `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result.Results, nil
}

// writeMemory writes a memory record to SSI asynchronously (fire-and-forget).
func writeMemory(ssiURL string, rec map[string]interface{}) {
	go func() {
		body, _ := json.Marshal(rec)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, ssiURL+"/memory", bytes.NewReader(body))
		if err != nil {
			log.Printf("[WARN] writeMemory build request: %v", err)
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			log.Printf("[WARN] writeMemory request: %v", err)
			return
		}
		resp.Body.Close()
	}()
}

// Phase 2: Prometheus and Loki fetchers.
func fetchPrometheusMetrics(ctx context.Context, prometheusURL string) (string, error) {
	queries := []struct{ name, q string }{
		{"memory_bytes", `container_memory_usage_bytes{name=~"kalpana-.*"}`},
		{"cpu_rate", `rate(container_cpu_usage_seconds_total{name=~"kalpana-.*"}[5m])`},
	}
	var sb strings.Builder
	client := &http.Client{Timeout: 10 * time.Second}
	for _, qry := range queries {
		u := fmt.Sprintf("%s/api/v1/query?query=%s", prometheusURL, qry.q)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			continue
		}
		resp, err := client.Do(req)
		if err != nil {
			continue
		}
		var pr struct {
			Data struct {
				Result []struct {
					Metric map[string]string `json:"metric"`
					Value  []interface{}     `json:"value"`
				} `json:"result"`
			} `json:"data"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()
		sb.WriteString(fmt.Sprintf("\n### %s\n", qry.name))
		for _, r := range pr.Data.Result {
			name := r.Metric["name"]
			if name == "" {
				name = r.Metric["container_label_com_docker_compose_service"]
			}
			var val string
			if len(r.Value) >= 2 {
				val = fmt.Sprintf("%v", r.Value[1])
			}
			sb.WriteString(fmt.Sprintf("  %s: %s\n", name, val))
		}
	}
	if sb.Len() == 0 {
		return "(no metrics available)", nil
	}
	return sb.String(), nil
}

func fetchLokiErrors(ctx context.Context, lokiURL string) (string, error) {
	end := time.Now().UnixNano()
	start := time.Now().Add(-1 * time.Hour).UnixNano()
	u := fmt.Sprintf("%s/loki/api/v1/query_range?query={job=\"docker\"}+|=+\"error\"&start=%d&end=%d&limit=20", lokiURL, start, end)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return "(loki unavailable)", nil
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "(loki unavailable)", nil
	}
	defer resp.Body.Close()
	var lr struct {
		Data struct {
			Result []struct {
				Values [][]string `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&lr); err != nil {
		return "(loki decode error)", nil
	}
	var sb strings.Builder
	for _, stream := range lr.Data.Result {
		for _, entry := range stream.Values {
			if len(entry) >= 2 {
				sb.WriteString(entry[1])
				sb.WriteString("\n")
			}
		}
	}
	if sb.Len() == 0 {
		return "(no error logs in the last hour)", nil
	}
	return sb.String(), nil
}

type COLService struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Status string `json:"status"`
	Image  string `json:"image"`
}

func colListServices(ctx context.Context, baseURL, token string) ([]COLService, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/services", nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", token)
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("col returned %d", resp.StatusCode)
	}
	var services []COLService
	if err := json.NewDecoder(resp.Body).Decode(&services); err != nil {
		return nil, err
	}
	return services, nil
}

func pingService(ctx context.Context, baseURL string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/health", nil)
	if err != nil {
		return err
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Command detection
// ---------------------------------------------------------------------------

var destructiveKeywords = []string{"stop", "delete", "remove", "destroy", "kill", "terminate"}
var safeKeywords = []string{"list", "status", "show", "get", "describe", "info"}

func detectCommand(reply string) (cmdType string, description string, found bool) {
	// Look for [ACTION: command_type | description]
	lower := strings.ToLower(reply)
	start := strings.Index(lower, "[action:")
	if start == -1 {
		return "", "", false
	}
	end := strings.Index(reply[start:], "]")
	if end == -1 {
		return "", "", false
	}
	inner := reply[start+len("[action:") : start+end]
	parts := strings.SplitN(inner, "|", 2)
	if len(parts) < 2 {
		cmdType = strings.TrimSpace(parts[0])
		description = cmdType
	} else {
		cmdType = strings.TrimSpace(parts[0])
		description = strings.TrimSpace(parts[1])
	}
	return cmdType, description, true
}

func isDestructive(cmdType string) bool {
	lower := strings.ToLower(cmdType)
	for _, kw := range destructiveKeywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

func isSafeCommand(cmdType string) bool {
	lower := strings.ToLower(cmdType)
	if strings.Contains(lower, "deploy") || strings.Contains(lower, "restart") || strings.Contains(lower, "scale") || strings.Contains(lower, "rollback") || strings.Contains(lower, "build") {
		return true
	}
	for _, kw := range safeKeywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// COL / AAF command execution
// ---------------------------------------------------------------------------

func parseParams(desc string) map[string]string {
	params := make(map[string]string)
	fields := strings.Fields(desc)
	for _, field := range fields {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) == 2 {
			k := strings.TrimSpace(parts[0])
			v := strings.TrimSpace(parts[1])
			v = strings.Trim(v, `"'`)
			params[k] = v
		}
	}
	return params
}

func executeCommandViaCOL(ctx context.Context, cfg Config, cmd *PendingCommand, token string) error {
	cmdType := strings.ToLower(cmd.CommandType)
	params := parseParams(cmd.Description)

	httpClient := &http.Client{Timeout: 60 * time.Second}

	switch cmdType {
	case "deploy_app", "register_app", "build_app":
		repo := params["repo"]
		if repo == "" {
			repo = params["git_repo"]
		}
		name := params["name"]
		if name == "" {
			name = params["app_name"]
		}
		if repo == "" || name == "" {
			return fmt.Errorf("deploy_app requires 'name' and 'repo' parameters")
		}

		platform := params["platform"]
		if platform == "" {
			platform = "web"
		}
		branch := params["branch"]
		if branch == "" {
			branch = "main"
		}
		domain := params["domain"]

		// 1. POST /apps/register
		registerURL := cfg.COLURL + "/apps/register"
		regBody, err := json.Marshal(map[string]string{
			"name":     name,
			"platform": platform,
			"git_repo": repo,
			"branch":   branch,
			"domain":   domain,
		})
		if err != nil {
			return fmt.Errorf("marshal register request: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, registerURL, bytes.NewReader(regBody))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", token)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("POST /apps/register failed: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("register app returned %d: %s", resp.StatusCode, string(b))
		}

		var regResp struct {
			ID       string `json:"id"`
			Status   string `json:"status"`
			Platform string `json:"platform"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&regResp); err != nil {
			return fmt.Errorf("decode register response: %w", err)
		}

		// 2. POST /apps/build
		buildURL := cfg.COLURL + "/apps/build"
		buildBody, err := json.Marshal(map[string]string{
			"app_id": regResp.ID,
		})
		if err != nil {
			return fmt.Errorf("marshal build request: %w", err)
		}

		reqBuild, err := http.NewRequestWithContext(ctx, http.MethodPost, buildURL, bytes.NewReader(buildBody))
		if err != nil {
			return err
		}
		reqBuild.Header.Set("Content-Type", "application/json")
		if token != "" {
			reqBuild.Header.Set("Authorization", token)
		}

		respBuild, err := httpClient.Do(reqBuild)
		if err != nil {
			return fmt.Errorf("POST /apps/build failed: %w", err)
		}
		defer respBuild.Body.Close()

		if respBuild.StatusCode >= 400 {
			b, _ := io.ReadAll(respBuild.Body)
			return fmt.Errorf("build app returned %d: %s", respBuild.StatusCode, string(b))
		}

		return nil

	case "remove_service", "stop_service", "delete_service":
		name := params["name"]
		if name == "" {
			name = params["service"]
		}
		if name == "" {
			return fmt.Errorf("remove_service requires 'name' parameter")
		}

		url := fmt.Sprintf("%s/services/%s", cfg.COLURL, name)
		req, err := http.NewRequestWithContext(ctx, http.MethodDelete, url, nil)
		if err != nil {
			return err
		}
		if token != "" {
			req.Header.Set("Authorization", token)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("DELETE /services/%s failed: %w", url, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("remove service returned %d: %s", resp.StatusCode, string(b))
		}
		return nil

	case "deploy_service", "deploy", "start_service":
		name := params["name"]
		if name == "" {
			name = params["service"]
		}
		image := params["image"]
		if name == "" || image == "" {
			return fmt.Errorf("deploy_service requires 'name' and 'image' parameters")
		}

		var ports []string
		if pStr, exists := params["ports"]; exists {
			for _, p := range strings.Split(pStr, ",") {
				if strings.TrimSpace(p) != "" {
					ports = append(ports, strings.TrimSpace(p))
				}
			}
		}

		var env []string
		if eStr, exists := params["env"]; exists {
			for _, e := range strings.Split(eStr, ",") {
				if strings.TrimSpace(e) != "" {
					env = append(env, strings.TrimSpace(e))
				}
			}
		}

		url := cfg.COLURL + "/services/deploy"
		deployBody, err := json.Marshal(map[string]interface{}{
			"name":  name,
			"image": image,
			"ports": ports,
			"env":   env,
		})
		if err != nil {
			return fmt.Errorf("marshal deploy request: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(deployBody))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", token)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("POST /services/deploy failed: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("deploy service returned %d: %s", resp.StatusCode, string(b))
		}
		return nil

	case "restart_service":
		name := params["name"]
		if name == "" {
			name = params["service"]
		}
		if name == "" {
			return fmt.Errorf("restart_service requires 'name' parameter")
		}

		url := fmt.Sprintf("%s/services/%s/restart", cfg.COLURL, name)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
		if err != nil {
			return err
		}
		if token != "" {
			req.Header.Set("Authorization", token)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("POST %s failed: %w", url, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("restart service returned %d: %s", resp.StatusCode, string(b))
		}
		return nil

	case "scale_service":
		name := params["name"]
		if name == "" {
			name = params["service"]
		}
		if name == "" {
			return fmt.Errorf("scale_service requires 'name' parameter")
		}

		replicasStr := params["replicas"]
		if replicasStr == "" {
			replicasStr = params["count"]
		}
		replicas := 1
		if replicasStr != "" {
			if rVal, err := strconv.Atoi(replicasStr); err == nil {
				replicas = rVal
			}
		}

		url := fmt.Sprintf("%s/services/%s/scale", cfg.COLURL, name)
		scaleBody, err := json.Marshal(map[string]interface{}{
			"replicas": replicas,
		})
		if err != nil {
			return fmt.Errorf("marshal scale request: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(scaleBody))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", token)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("POST %s failed: %w", url, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("scale service returned %d: %s", resp.StatusCode, string(b))
		}
		return nil

	case "rollback_service":
		name := params["name"]
		if name == "" {
			name = params["service"]
		}
		if name == "" {
			return fmt.Errorf("rollback_service requires 'name' parameter")
		}

		url := fmt.Sprintf("%s/services/%s/rollback", cfg.COLURL, name)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
		if err != nil {
			return err
		}
		if token != "" {
			req.Header.Set("Authorization", token)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("POST %s failed: %w", url, err)
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 400 {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("rollback service returned %d: %s", resp.StatusCode, string(b))
		}
		return nil

	default:
		// Fallback to post to raw /commands if there is any other command type (just in case)
		url := cfg.COLURL + "/commands"
		body, _ := json.Marshal(map[string]interface{}{
			"command_type": cmd.CommandType,
			"payload":      cmd.Payload,
			"description":  cmd.Description,
		})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", token)
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode >= 400 {
			b, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("col command returned %d: %s", resp.StatusCode, string(b))
		}
		return nil
	}
}

// ---------------------------------------------------------------------------
// JWT middleware
// ---------------------------------------------------------------------------

func jwtMiddleware(silURL string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "missing or invalid Authorization header"})
				return
			}
			token := strings.TrimPrefix(authHeader, "Bearer ")

			ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
			defer cancel()

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, silURL+"/validate", nil)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to build validation request"})
				return
			}
			req.Header.Set("Authorization", "Bearer "+token)
			resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
			if err != nil {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "token validation service unavailable"})
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid or expired token"})
				return
			}

			// Parse claims for request context enrichment (best-effort)
			p := jwt.NewParser()
			claims := jwt.MapClaims{}
			_, _, _ = p.ParseUnverified(token, claims)
			ctx2 := context.WithValue(r.Context(), ctxKeyClaims{}, claims)
			next.ServeHTTP(w, r.WithContext(ctx2))
		})
	}
}

type ctxKeyClaims struct{}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

type Handler struct {
	cfg    Config
	store  *Store
	nvidia *NvidiaClient
	nc     *nats.Conn // may be nil if NATS unavailable
}

func NewHandler(cfg Config, store *Store, nvidia *NvidiaClient, nc *nats.Conn) *Handler {
	return &Handler{cfg: cfg, store: store, nvidia: nvidia, nc: nc}
}

// POST /chat
func (h *Handler) chat(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Message   string `json:"message"`
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Message) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "message is required"})
		return
	}

	// Resolve or create session
	var session *ChatSession
	if req.SessionID != "" {
		if s, ok := h.store.GetSession(req.SessionID); ok {
			session = s
		}
	}
	if session == nil {
		session = &ChatSession{
			ID:        newID(),
			Messages:  []Message{},
			CreatedAt: time.Now().UTC(),
		}
	}

	ctx := r.Context()
	token := r.Header.Get("Authorization")

	// --- RAG Step 1: SSI search ---
	var ragContext strings.Builder
	ssiResults, err := ssiSearch(ctx, h.cfg.SSIURL, req.Message, token)
	if err != nil {
		log.Printf("[WARN] SSI search failed: %v", err)
	} else if len(ssiResults) > 0 {
		ragContext.WriteString("## Relevant Knowledge Base Documents\n")
		for i, res := range ssiResults {
			ragContext.WriteString(fmt.Sprintf("%d. %s\n", i+1, res.Content))
		}
		ragContext.WriteString("\n")
	}

	// --- RAG Step 1b (Phase 2): Episodic memory search ---
	memories, memErr := ssiMemorySearch(ctx, h.cfg.SSIURL, req.Message, token)
	if memErr != nil {
		log.Printf("[WARN] Memory search failed: %v", memErr)
	} else if len(memories) > 0 {
		ragContext.WriteString("## Relevant Past Interactions (Episodic Memory)\n")
		for i, m := range memories {
			ragContext.WriteString(fmt.Sprintf("%d. [%s] %s\n", i+1, m.Type, m.Content))
		}
		ragContext.WriteString("\n")
	}

	// --- RAG Step 2: COL infrastructure state ---
	services, err := colListServices(ctx, h.cfg.COLURL, token)
	if err != nil {
		log.Printf("[WARN] COL list services failed: %v", err)
	} else {
		ragContext.WriteString("## Current Infrastructure State\n")
		for _, svc := range services {
			ragContext.WriteString(fmt.Sprintf("- %s (%s): %s\n", svc.Name, svc.ID, svc.Status))
		}
		ragContext.WriteString("\n")
	}

	// --- Build messages for NVIDIA ---
	nvidiaMessages := []NvidiaMessage{
		{Role: "system", Content: systemPrompt},
	}
	if ragContext.Len() > 0 {
		nvidiaMessages = append(nvidiaMessages, NvidiaMessage{
			Role:    "system",
			Content: "Context information:\n" + ragContext.String(),
		})
	}
	// Inject conversation history (last 10 messages to stay within context)
	historyStart := 0
	if len(session.Messages) > 10 {
		historyStart = len(session.Messages) - 10
	}
	for _, msg := range session.Messages[historyStart:] {
		nvidiaMessages = append(nvidiaMessages, NvidiaMessage{
			Role:    msg.Role,
			Content: msg.Content,
		})
	}
	// Append current user message
	nvidiaMessages = append(nvidiaMessages, NvidiaMessage{
		Role:    "user",
		Content: req.Message,
	})

	// --- NVIDIA API call ---
	reply, err := h.nvidia.Chat(ctx, nvidiaMessages)
	if err != nil {
		log.Printf("[ERROR] NVIDIA chat failed: %v", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "AI inference failed: " + err.Error()})
		return
	}

	// --- Store messages in session ---
	now := time.Now().UTC()
	session.Messages = append(session.Messages,
		Message{Role: "user", Content: req.Message, Timestamp: now},
		Message{Role: "assistant", Content: reply, Timestamp: now},
	)
	h.store.UpsertSession(session)
	if err := h.store.save(); err != nil {
		log.Printf("[WARN] state save failed: %v", err)
	}

	// --- Phase 2: Write interaction to episodic memory (async) ---
	writeMemory(h.cfg.SSIURL, map[string]interface{}{
		"type":       "chat_interaction",
		"source":     "aicp",
		"content":    "User: " + req.Message + "\nAI: " + reply,
		"session_id": session.ID,
		"tags":       []string{"chat"},
	})

	// --- Command detection ---
	var pendingCmd *PendingCommand
	cmdType, desc, found := detectCommand(reply)
	if found {
		if isDestructive(cmdType) {
			// Create pending command requiring confirmation
			pc := &PendingCommand{
				ID:                   newID(),
				SessionID:            session.ID,
				CommandType:          cmdType,
				Payload:              json.RawMessage(`{}`),
				Description:          desc,
				RequiresConfirmation: true,
				CreatedAt:            time.Now().UTC(),
			}
			h.store.AddPending(pc)
			if err := h.store.save(); err != nil {
				log.Printf("[WARN] state save after pending command: %v", err)
			}
			pendingCmd = pc
			h.publishEvent("aicp.command.pending", pc)
		} else if isSafeCommand(cmdType) {
			token := r.Header.Get("Authorization")
			// Execute safe command immediately (async, non-blocking to response)
			immediateCmd := &PendingCommand{
				ID:                   newID(),
				SessionID:            session.ID,
				CommandType:          cmdType,
				Payload:              json.RawMessage(`{}`),
				Description:          desc,
				RequiresConfirmation: false,
				CreatedAt:            time.Now().UTC(),
			}
			go func() {
				execCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				if err := executeCommandViaCOL(execCtx, h.cfg, immediateCmd, token); err != nil {
					log.Printf("[WARN] safe command execution failed: %v", err)
				}
			}()
		}
	}

	resp := map[string]interface{}{
		"reply":      reply,
		"session_id": session.ID,
	}
	if pendingCmd != nil {
		resp["pending_command"] = pendingCmd
	}
	writeJSON(w, http.StatusOK, resp)
}

// GET /sessions
func (h *Handler) listSessions(w http.ResponseWriter, r *http.Request) {
	sessions := h.store.ListSessions()
	writeJSON(w, http.StatusOK, sessions)
}

// GET /sessions/{id}/history
func (h *Handler) getSessionHistory(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	sess, ok := h.store.GetSession(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}
	writeJSON(w, http.StatusOK, sess.Messages)
}

// DELETE /sessions/{id}
func (h *Handler) deleteSession(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if _, ok := h.store.GetSession(id); !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "session not found"})
		return
	}
	h.store.DeleteSession(id)
	if err := h.store.save(); err != nil {
		log.Printf("[WARN] state save after delete: %v", err)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// GET /pending
func (h *Handler) listPending(w http.ResponseWriter, r *http.Request) {
	cmds := h.store.ListPending()
	writeJSON(w, http.StatusOK, cmds)
}

// POST /pending/{id}/confirm
func (h *Handler) confirmPending(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	cmd, ok := h.store.GetPending(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "pending command not found"})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()

	token := r.Header.Get("Authorization")
	if err := executeCommandViaCOL(ctx, h.cfg, cmd, token); err != nil {
		log.Printf("[ERROR] command execution failed: %v", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "command execution failed: " + err.Error()})
		return
	}

	h.store.RemovePending(id)
	if err := h.store.save(); err != nil {
		log.Printf("[WARN] state save after confirm: %v", err)
	}
	h.publishEvent("aicp.command.executed", cmd)
	writeJSON(w, http.StatusOK, map[string]string{"status": "executed", "command_id": id})
}

// POST /pending/{id}/reject
func (h *Handler) rejectPending(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	cmd, ok := h.store.GetPending(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "pending command not found"})
		return
	}
	h.store.RemovePending(id)
	if err := h.store.save(); err != nil {
		log.Printf("[WARN] state save after reject: %v", err)
	}
	h.publishEvent("aicp.command.rejected", cmd)
	writeJSON(w, http.StatusOK, map[string]string{"status": "rejected", "command_id": id})
}

// GET /status
func (h *Handler) status(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	nvidiaStatus := "connected"
	if err := h.nvidia.Ping(ctx); err != nil {
		nvidiaStatus = "error: " + err.Error()
	}

	ssiStatus := "ok"
	if err := pingService(ctx, h.cfg.SSIURL); err != nil {
		ssiStatus = "error: " + err.Error()
	}

	colStatus := "ok"
	if err := pingService(ctx, h.cfg.COLURL); err != nil {
		colStatus = "error: " + err.Error()
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"nvidia_api": nvidiaStatus,
		"ssi":        ssiStatus,
		"col":        colStatus,
		"model":      h.cfg.NvidiaChatModel,
	})
}

// ---------------------------------------------------------------------------
// Phase 2: AI Anomaly Detection
// ---------------------------------------------------------------------------

var anomalyPrompt = `You are an infrastructure anomaly detection system for KalpanaOS. Analyze the following telemetry data and identify any anomalies.

For each anomaly found, output EXACTLY this format on its own line:
ANOMALY: severity=<LOW|MEDIUM|HIGH|CRITICAL> title=<short title without quotes> services=<comma-separated list> description=<one sentence explanation>

If no anomalies are detected, output exactly: NO_ANOMALIES

Be concise and technical. Only flag genuine issues, not normal operational metrics.`

func (h *Handler) runDiagnosis(ctx context.Context, windowMinutes int) (string, []*AnomalyRecord, error) {
	metrics, err := fetchPrometheusMetrics(ctx, h.cfg.PrometheusURL)
	if err != nil {
		log.Printf("[WARN] Prometheus fetch failed: %v", err)
		metrics = "(Prometheus unavailable)"
	}

	logs, err := fetchLokiErrors(ctx, h.cfg.LokiURL)
	if err != nil {
		log.Printf("[WARN] Loki fetch failed: %v", err)
		logs = "(Loki unavailable)"
	}

	prompt := fmt.Sprintf("%s\n\nTIME WINDOW: Last %d minutes\n\nMETRICS:\n%s\n\nERROR LOGS (last hour):\n%s",
		anomalyPrompt, windowMinutes, metrics, logs)

	llmMessages := []NvidiaMessage{
		{Role: "user", Content: prompt},
	}

	report, err := h.nvidia.Chat(ctx, llmMessages)
	if err != nil {
		return "", nil, fmt.Errorf("LLM analysis failed: %w", err)
	}

	// Parse anomaly records from report
	var anomalies []*AnomalyRecord
	for _, line := range strings.Split(report, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "ANOMALY:") {
			continue
		}
		parts := strings.TrimPrefix(line, "ANOMALY:")
		a := &AnomalyRecord{
			ID:         newID(),
			DetectedAt: time.Now().UTC(),
			Severity:   "LOW",
		}
		
		// Extract description first if present (everything after description=)
		header := parts
		if idx := strings.Index(parts, "description="); idx >= 0 {
			a.Description = strings.TrimSpace(parts[idx+len("description="):])
			header = parts[:idx]
		}
		
		// Parse key=value pairs from the header part
		for _, kv := range strings.Fields(header) {
			idx := strings.Index(kv, "=")
			if idx < 0 {
				continue
			}
			k, v := kv[:idx], kv[idx+1:]
			switch k {
			case "severity":
				a.Severity = strings.ToUpper(v)
			case "title":
				a.Title = strings.Trim(v, `"'`)
			case "services":
				a.ServicesAffected = strings.Split(v, ",")
			}
		}
		if a.Title != "" {
			anomalies = append(anomalies, a)
			h.store.AddAnomaly(a)
			h.publishEvent("kalpana.anomalies", a)
			// Write to episodic memory
			writeMemory(h.cfg.SSIURL, map[string]interface{}{
				"type":    "anomaly",
				"source":  "aicp",
				"content": fmt.Sprintf("[%s] %s: %s", a.Severity, a.Title, a.Description),
				"tags":    []string{"anomaly", strings.ToLower(a.Severity)},
			})
		}
	}

	if err := h.store.save(); err != nil {
		log.Printf("[WARN] state save after diagnosis: %v", err)
	}

	return report, anomalies, nil
}

// POST /diagnose
func (h *Handler) diagnose(w http.ResponseWriter, r *http.Request) {
	var req struct {
		WindowMinutes int `json:"window_minutes"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.WindowMinutes <= 0 {
		req.WindowMinutes = 30
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Minute)
	defer cancel()

	report, anomalies, err := h.runDiagnosis(ctx, req.WindowMinutes)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"report":             report,
		"anomalies_detected": len(anomalies),
		"anomalies":          anomalies,
		"window_minutes":     req.WindowMinutes,
		"scanned_at":         time.Now().UTC().Format(time.RFC3339),
	})
}

// GET /anomalies
func (h *Handler) listAnomalies(w http.ResponseWriter, r *http.Request) {
	resolvedParam := r.URL.Query().Get("resolved")
	var filter *bool
	if resolvedParam != "" {
		v := resolvedParam == "true"
		filter = &v
	}
	anomalies := h.store.ListAnomalies(filter)
	if anomalies == nil {
		anomalies = []*AnomalyRecord{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"anomalies": anomalies, "count": len(anomalies)})
}

// GET /anomalies/{id}
func (h *Handler) getAnomaly(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	a, ok := h.store.GetAnomaly(id)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "anomaly not found"})
		return
	}
	writeJSON(w, http.StatusOK, a)
}

// POST /anomalies/{id}/resolve
func (h *Handler) resolveAnomaly(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req struct {
		Note string `json:"note"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if !h.store.ResolveAnomaly(id, req.Note) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "anomaly not found"})
		return
	}
	if err := h.store.save(); err != nil {
		log.Printf("[WARN] state save after resolve: %v", err)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "resolved", "id": id})
}

// GET /memory — proxy to SSI memory list
func (h *Handler) listMemory(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	url := h.cfg.SSIURL + "/memory?type=chat_interaction&limit=50"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// DELETE /memory/{id} — proxy to SSI memory delete
func (h *Handler) deleteMemory(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, h.cfg.SSIURL+"/memory/"+id, nil)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// ---------------------------------------------------------------------------
// NATS publisher
// ---------------------------------------------------------------------------

func (h *Handler) publishEvent(subject string, payload interface{}) {
	if h.nc == nil {
		return
	}
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[WARN] NATS marshal for %s: %v", subject, err)
		return
	}
	if err := h.nc.Publish(subject, data); err != nil {
		log.Printf("[WARN] NATS publish %s: %v", subject, err)
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[WARN] writeJSON encode error: %v", err)
	}
}

func newID() string {
	return fmt.Sprintf("%d-%d", time.Now().UnixNano(), pseudoRandN())
}

var pseudoRandCounter uint64
var pseudoRandMu sync.Mutex

func pseudoRandN() uint64 {
	pseudoRandMu.Lock()
	defer pseudoRandMu.Unlock()
	pseudoRandCounter++
	return pseudoRandCounter
}

// ---------------------------------------------------------------------------
// Prometheus middleware
// ---------------------------------------------------------------------------

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (sr *statusRecorder) WriteHeader(status int) {
	sr.status = status
	sr.ResponseWriter.WriteHeader(status)
}

func prometheusMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		elapsed := time.Since(start).Seconds()
		path := r.URL.Path
		httpRequestsTotal.WithLabelValues(r.Method, path, fmt.Sprintf("%d", rec.status)).Inc()
		httpRequestDuration.WithLabelValues(r.Method, path).Observe(elapsed)
	})
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	cfg := loadConfig()

	// State store
	store, err := NewStore(cfg.StatePath)
	if err != nil {
		log.Fatalf("Failed to initialize state store: %v", err)
	}

	// NVIDIA client
	nvidia := NewNvidiaClient(cfg.NvidiaAPIKey, cfg.NvidiaAPIBase, cfg.NvidiaChatModel, cfg.OllamaURL, cfg.OllamaModel)

	// NATS connection (optional — service still works without it)
	var nc *nats.Conn
	nc, err = nats.Connect(cfg.NATSURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(5),
		nats.ReconnectWait(2*time.Second),
		nats.Timeout(5*time.Second),
	)
	if err != nil {
		log.Printf("[WARN] NATS connection failed (continuing without NATS): %v", err)
		nc = nil
	} else {
		log.Printf("[INFO] Connected to NATS at %s", cfg.NATSURL)
		defer nc.Close()
	}

	h := NewHandler(cfg, store, nvidia, nc)

	// Router
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(prometheusMiddleware)

	// Public endpoints
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "service": "aicp"})
	})
	r.Handle("/metrics", promhttp.Handler())

	// Protected endpoints
	r.Group(func(r chi.Router) {
		r.Use(jwtMiddleware(cfg.SILURL))

		r.Post("/chat", h.chat)
		r.Post("/chat/agent-mode", h.chatAgentMode) // Phase 3: explicit multi-agent mode

		r.Get("/sessions", h.listSessions)
		r.Get("/sessions/{id}/history", h.getSessionHistory)
		r.Delete("/sessions/{id}", h.deleteSession)

		r.Get("/pending", h.listPending)
		r.Post("/pending/{id}/confirm", h.confirmPending)
		r.Post("/pending/{id}/reject", h.rejectPending)

		r.Get("/status", h.status)

		// Phase 2: Anomaly Detection
		r.Post("/diagnose", h.diagnose)
		r.Get("/anomalies", h.listAnomalies)
		r.Get("/anomalies/{id}", h.getAnomaly)
		r.Post("/anomalies/{id}/resolve", h.resolveAnomaly)

		// Phase 2: Memory
		r.Get("/memory", h.listMemory)
		r.Delete("/memory/{id}", h.deleteMemory)

		// Phase 3: Orchestrations
		r.Get("/orchestrations", h.listOrchestrations)
		r.Post("/orchestrate", h.orchestrate)
	})

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      r,
		ReadTimeout:  120 * time.Second, // allow long LLM responses
		WriteTimeout: 120 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// Phase 2: Background anomaly detection (every 30 minutes)
	go func() {
		ticker := time.NewTicker(30 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			log.Println("[INFO] Running scheduled anomaly detection scan")
			scanCtx, scanCancel := context.WithTimeout(context.Background(), 2*time.Minute)
			report, anomalies, err := h.runDiagnosis(scanCtx, 30)
			scanCancel()
			if err != nil {
				log.Printf("[WARN] Scheduled diagnosis failed: %v", err)
				continue
			}
			log.Printf("[INFO] Anomaly scan complete: %d anomalies detected (report len: %d chars)", len(anomalies), len(report))
		}
	}()

	go func() {
		log.Printf("[INFO] AICP listening on :%s (model: %s)", cfg.Port, cfg.NvidiaChatModel)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-quit
	log.Println("[INFO] Shutting down AICP...")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("[ERROR] Graceful shutdown failed: %v", err)
	}
	// Final state flush
	if err := store.save(); err != nil {
		log.Printf("[WARN] Final state save failed: %v", err)
	}
	log.Println("[INFO] AICP stopped")
}

// ─── Phase 3: Multi-Agent Chat + Orchestration ────────────────────────────────

var multiAgentKeywords = []string{
	"comprehensive", "full analysis", "complete report", "investigate",
	"all agents", "parallel analysis", "everything", "deep dive", "thorough",
	"synthesize", "predict and report", "analyze and scale",
}

func isMultiAgentQuery(msg string) bool {
	lower := strings.ToLower(msg)
	for _, kw := range multiAgentKeywords {
		if strings.Contains(lower, kw) {
			return true
		}
	}
	return false
}

func (h *Handler) callOrchestrator(ctx context.Context, token, goal string) (string, error) {
	body, _ := json.Marshal(map[string]string{"goal": goal})
	req, err := http.NewRequestWithContext(ctx, "POST",
		h.cfg.ORCHESTRATORURL+"/orchestrate", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := (&http.Client{Timeout: 10 * time.Minute}).Do(req)
	if err != nil {
		return "", fmt.Errorf("orchestrator error: %w", err)
	}
	defer resp.Body.Close()

	var plan struct {
		FinalOutput string `json:"final_output"`
		Status      string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&plan); err != nil {
		return "", err
	}
	return plan.FinalOutput, nil
}

// POST /chat/agent-mode — explicit multi-agent mode
func (h *Handler) chatAgentMode(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Goal      string `json:"goal"`
		SessionID string `json:"session_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Goal) == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "goal is required"})
		return
	}

	token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	result, err := h.callOrchestrator(ctx, token, req.Goal)
	if err != nil {
		log.Printf("[WARN] Orchestrator call failed: %v — falling back to single LLM", err)
		reply, chatErr := h.nvidia.Chat(ctx, []NvidiaMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: req.Goal},
		})
		if chatErr != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "all inference modes failed"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"reply": reply, "mode": "single-agent-fallback",
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"reply": result, "mode": "multi-agent",
	})
}

// GET /orchestrations — proxy to Orchestrator /plans
func (h *Handler) listOrchestrations(w http.ResponseWriter, r *http.Request) {
	req, _ := http.NewRequestWithContext(r.Context(), "GET",
		h.cfg.ORCHESTRATORURL+"/plans", nil)
	req.Header.Set("Authorization", r.Header.Get("Authorization"))
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "orchestrator unreachable"})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

// POST /orchestrate — proxy to Orchestrator /orchestrate
func (h *Handler) orchestrate(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	req, _ := http.NewRequestWithContext(r.Context(), "POST",
		h.cfg.ORCHESTRATORURL+"/orchestrate", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", r.Header.Get("Authorization"))
	resp, err := (&http.Client{Timeout: 10 * time.Minute}).Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "orchestrator unreachable"})
		return
	}
	defer resp.Body.Close()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}

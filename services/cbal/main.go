package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/nats-io/nats.go"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Port         string
	DBPath       string
	NatsURL      string
	RedpandaURLs []string
	MinioURL     string
	MinioUser    string
	MinioPass    string
	SilURL       string
	IkgURL       string
	AicpURL      string
	PgeURL       string
}

func loadConfig() Config {
	getEnv := func(k, d string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return d
	}
	rpBrokers := getEnv("REDPANDA_BROKERS", "redpanda:9092")
	return Config{
		Port:         getEnv("PORT", "8010"),
		DBPath:       getEnv("DB_PATH", "/data/cbal.db"),
		NatsURL:      getEnv("NATS_URL", "nats://nats:4222"),
		RedpandaURLs: strings.Split(rpBrokers, ","),
		MinioURL:     getEnv("MINIO_URL", "http://minio:9000"),
		MinioUser:    getEnv("MINIO_ROOT_USER", "kalpana-admin"),
		MinioPass:    getEnv("MINIO_ROOT_PASSWORD", "kalpana-secret-2026"),
		SilURL:       getEnv("SIL_URL", "http://sil:8001"),
		IkgURL:       getEnv("IKG_URL", "http://ikg:8008"),
		AicpURL:      getEnv("AICP_URL", "http://aicp:8004"),
		PgeURL:       getEnv("PGE_URL", "http://pge:8007"),
	}
}

// Global structs for event contracts

type TelemetryMetric struct {
	CorrelationID string            `json:"correlation_id"`
	Timestamp     time.Time         `json:"timestamp"`
	NodeID        string            `json:"node_id"`
	AgentID       string            `json:"agent_id"`
	MetricName    string            `json:"metric_name"`
	Value         float64           `json:"value"`
	Region        string            `json:"region"`
	Metadata      map[string]string `json:"metadata"`
}

type OrchestrationEvent struct {
	CorrelationID string    `json:"correlation_id"`
	Timestamp     time.Time `json:"timestamp"`
	NodeID        string    `json:"node_id"`
	Action        string    `json:"action"`
	ServiceName   string    `json:"service_name"`
	Image         string    `json:"image"`
	Status        string    `json:"status"`
	Actor         string    `json:"actor"`
}

type AnomalyEvent struct {
	CorrelationID string    `json:"correlation_id"`
	Timestamp     time.Time `json:"timestamp"`
	NodeID        string    `json:"node_id"`
	AnomalyID     string    `json:"anomaly_id"`
	Type          string    `json:"type"`
	Description   string    `json:"description"`
	Severity      string    `json:"severity"`
	Resolved      bool      `json:"resolved"`
}

type Server struct {
	cfg      Config
	db       *sql.DB
	nc       *nats.Conn
	rClient  *kgo.Client
	httpClient *http.Client
	mu       sync.Mutex
	remediationCounters map[string]int // tracks restarts per service for remediation storm
}

func newServer(cfg Config) (*Server, error) {
	// Create data directory
	dbDir := "/data"
	if strings.Contains(cfg.DBPath, "/") {
		dbDir = cfg.DBPath[:strings.LastIndex(cfg.DBPath, "/")]
	}
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		log.Printf("[CBAL] DB directory error: %v", err)
	}

	// 1. Initialize DuckDB
	db, err := sql.Open("duckdb", cfg.DBPath)
	if err != nil {
		return nil, fmt.Errorf("duckdb open failed: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("duckdb ping failed: %w", err)
	}

	// Apply memory limits to DuckDB
	if _, err := db.Exec("SET max_memory = '100MB';"); err != nil {
		log.Printf("[WARN] Failed to set max_memory on DuckDB: %v", err)
	}
	if _, err := db.Exec("SET threads = 1;"); err != nil {
		log.Printf("[WARN] Failed to set threads on DuckDB: %v", err)
	}
	if _, err := db.Exec("SET temp_directory = '/data/duckdb_temp';"); err != nil {
		log.Printf("[WARN] Failed to set temp_directory on DuckDB: %v", err)
	}
	if _, err := db.Exec("SET autoinstall_known_extensions = true;"); err != nil {
		log.Printf("[WARN] Failed to set autoinstall_known_extensions on DuckDB: %v", err)
	}
	if _, err := db.Exec("SET autoload_known_extensions = true;"); err != nil {
		log.Printf("[WARN] Failed to set autoload_known_extensions on DuckDB: %v", err)
	}
	if _, err := db.Exec("INSTALL httpfs;"); err != nil {
		log.Printf("[WARN] Failed to install httpfs on DuckDB: %v", err)
	}
	if _, err := db.Exec("LOAD httpfs;"); err != nil {
		log.Printf("[WARN] Failed to load httpfs on DuckDB: %v", err)
	}

	// Setup schemas
	schemas := []string{
		`CREATE TABLE IF NOT EXISTS metric_history (
			timestamp TIMESTAMP,
			correlation_id VARCHAR,
			node_id VARCHAR,
			agent_id VARCHAR,
			metric_name VARCHAR,
			value DOUBLE,
			region VARCHAR
		);`,
		`CREATE TABLE IF NOT EXISTS orchestration_history (
			timestamp TIMESTAMP,
			correlation_id VARCHAR,
			node_id VARCHAR,
			action VARCHAR,
			service_name VARCHAR,
			image VARCHAR,
			status VARCHAR,
			actor VARCHAR
		);`,
		`CREATE TABLE IF NOT EXISTS anomaly_history (
			timestamp TIMESTAMP,
			anomaly_id VARCHAR,
			node_id VARCHAR,
			type VARCHAR,
			description VARCHAR,
			severity VARCHAR,
			resolved BOOLEAN
		);`,
	}
	for _, schema := range schemas {
		if _, err := db.Exec(schema); err != nil {
			return nil, fmt.Errorf("schema creation failed: %w", err)
		}
	}

	return &Server{
		cfg:                 cfg,
		db:                  db,
		httpClient:          &http.Client{Timeout: 10 * time.Second},
		remediationCounters: make(map[string]int),
	}, nil
}

// ─── NATS Ingest & Redpanda Event Backbone ──────────────────────────────────────

func (s *Server) initNATS() {
	var err error
	for i := 0; i < 10; i++ {
		s.nc, err = nats.Connect(s.cfg.NatsURL)
		if err == nil {
			break
		}
		log.Printf("[CBAL] NATS connection retry %d/10...", i+1)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Printf("[ERROR] NATS connection failed: %v", err)
		return
	}
	log.Printf("[CBAL] Connected to NATS JetStream at %s", s.cfg.NatsURL)

	// Subscribe to internal topics and pipeline to Redpanda
	s.nc.Subscribe("kalpana.col.events", func(m *nats.Msg) {
		s.pipeToRedpanda("kalpana.orchestration.events", m.Data)
	})

	s.nc.Subscribe("kalpana.agent.heartbeat", func(m *nats.Msg) {
		// Convert agent heartbeat to metric point
		var hb struct {
			AgentID string    `json:"agent_id"`
			Status  string    `json:"status"`
			Time    time.Time `json:"time"`
		}
		if err := json.Unmarshal(m.Data, &hb); err == nil {
			metric := TelemetryMetric{
				CorrelationID: fmt.Sprintf("hb-%s-%d", hb.AgentID, hb.Time.Unix()),
				Timestamp:     hb.Time,
				NodeID:        "local-node",
				AgentID:       hb.AgentID,
				MetricName:    "agent.heartbeat.pulse",
				Value:         1.0,
				Region:        "local",
			}
			payload, _ := json.Marshal(metric)
			s.pipeToRedpanda("kalpana.telemetry.metrics", payload)
		}
	})

	s.nc.Subscribe("kalpana.alerts", func(m *nats.Msg) {
		s.pipeToRedpanda("kalpana.remediation.actions", m.Data)
	})

	s.nc.Subscribe("kalpana.anomalies", func(m *nats.Msg) {
		s.pipeToRedpanda("kalpana.remediation.actions", m.Data)
	})
}

func (s *Server) initRedpanda() {
	var err error
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.cfg.RedpandaURLs...),
		kgo.AllowAutoTopicCreation(),
	}

	for i := 0; i < 15; i++ {
		s.rClient, err = kgo.NewClient(opts...)
		if err == nil {
			// Verify connectivity
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			err = s.rClient.Ping(ctx)
			cancel()
			if err == nil {
				break
			}
		}
		log.Printf("[CBAL] Redpanda connection retry %d/15...", i+1)
		time.Sleep(3 * time.Second)
	}

	if err != nil {
		log.Printf("[ERROR] Redpanda connection failed: %v", err)
		return
	}
	log.Printf("[CBAL] Connected to Redpanda at %v", s.cfg.RedpandaURLs)

	// Start Go consumer workers
	go s.startConsumerWorkers()
}

func (s *Server) pipeToRedpanda(topic string, payload []byte) {
	if s.rClient == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	record := &kgo.Record{Topic: topic, Value: payload}
	if err := s.rClient.ProduceSync(ctx, record).FirstErr(); err != nil {
		log.Printf("[WARN] Redpanda produce fail to topic %s: %v", topic, err)
	}
}

// ─── Go-Based Stream Processing Engine ──────────────────────────────────────

func (s *Server) startConsumerWorkers() {
	log.Printf("[CBAL] Starting Go consumer workers...")
	topics := []string{
		"kalpana.telemetry.metrics",
		"kalpana.orchestration.events",
		"kalpana.remediation.actions",
	}

	// Setup consumer client
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.cfg.RedpandaURLs...),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumerGroup("cbal-workers"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("Failed to create Redpanda consumer: %v", err)
	}
	defer cl.Close()

	for {
		ctx := context.Background()
		fetches := cl.PollRecords(ctx, 50) // Micro-batch polls (up to 50 records)
		if fetches.IsClientClosed() {
			break
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			s.processEvent(record.Topic, record.Value)
		}
	}
}

func (s *Server) processEvent(topic string, value []byte) {
	log.Printf("[CBAL] Processing event from topic %s", topic)

	switch topic {
	case "kalpana.telemetry.metrics":
		var m TelemetryMetric
		if err := json.Unmarshal(value, &m); err == nil {
			// Graph enrichment step
			m.NodeID = s.enrichWithGraph(m.NodeID)
			s.writeMetric(m)
			s.detectMetricAnomaly(m)
		}
	case "kalpana.orchestration.events":
		var e OrchestrationEvent
		if err := json.Unmarshal(value, &e); err == nil {
			e.NodeID = s.enrichWithGraph(e.NodeID)
			s.writeOrchestration(e)
		}
	case "kalpana.remediation.actions":
		// Remediation events can double as anomalies or logs
		var a AnomalyEvent
		if err := json.Unmarshal(value, &a); err == nil {
			s.writeAnomaly(a)
			s.detectRemediationStorm(a)
		}
	}
}

func (s *Server) enrichWithGraph(nodeID string) string {
	if nodeID == "" {
		return "unknown-node"
	}
	// Query IKG for node properties (e.g. location, hostname)
	url := fmt.Sprintf("%s/nodes/%s", s.cfg.IkgURL, nodeID)
	req, _ := http.NewRequest("GET", url, nil)
	resp, err := s.httpClient.Do(req)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			var node struct {
				ID         string            `json:"id"`
				Type       string            `json:"type"`
				Properties map[string]string `json:"properties"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&node); err == nil {
				if region, ok := node.Properties["region"]; ok {
					return nodeID + " (" + region + ")"
				}
			}
		}
	}
	return nodeID
}

// Real-Time Anomaly Detection & Remediation Storms
func (s *Server) detectRemediationStorm(a AnomalyEvent) {
	if strings.Contains(strings.ToLower(a.Description), "restart") || strings.Contains(strings.ToLower(a.Type), "remediation") {
		s.mu.Lock()
		s.remediationCounters[a.NodeID]++
		count := s.remediationCounters[a.NodeID]
		s.mu.Unlock()

		if count > 5 {
			log.Printf("[ALERT] Remediation Storm detected on node %s! Action count: %d", a.NodeID, count)
			// Trigger PGE policy override / notify NATS
			alertPayload, _ := json.Marshal(map[string]interface{}{
				"event":       "remediation_storm",
				"node_id":     a.NodeID,
				"description": fmt.Sprintf("Critical: Service restart loop exceeding limits (%d crashes). PGE governance action recommended.", count),
				"timestamp":   time.Now().UTC(),
			})
			s.nc.Publish("kalpana.alerts", alertPayload)
		}
	}
}

func (s *Server) detectMetricAnomaly(m TelemetryMetric) {
	// Simple threshold check: CPU > 95% or Memory > 90%
	if m.MetricName == "host.memory.usage.pct" && m.Value > 90.0 {
		log.Printf("[ANOMALY] Memory threshold exceeded on %s: %.2f%%", m.NodeID, m.Value)
		s.writeAnomaly(AnomalyEvent{
			CorrelationID: m.CorrelationID,
			Timestamp:     time.Now().UTC(),
			NodeID:        m.NodeID,
			AnomalyID:     "mem-high-" + strconv.FormatInt(time.Now().Unix(), 10),
			Type:          "RESOURCE_SATURATION",
			Description:   fmt.Sprintf("Memory usage is critically high: %.2f%%", m.Value),
			Severity:      "CRITICAL",
			Resolved:      false,
		})
	}
}

// ─── SQL Writers (DuckDB) ───────────────────────────────────────────────────

func (s *Server) writeMetric(m TelemetryMetric) {
	query := `INSERT INTO metric_history (timestamp, correlation_id, node_id, agent_id, metric_name, value, region) VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err := s.db.Exec(query, m.Timestamp, m.CorrelationID, m.NodeID, m.AgentID, m.MetricName, m.Value, m.Region)
	if err != nil {
		log.Printf("[WARN] Failed to write metric to DuckDB: %v", err)
	}
}

func (s *Server) writeOrchestration(e OrchestrationEvent) {
	query := `INSERT INTO orchestration_history (timestamp, correlation_id, node_id, action, service_name, image, status, actor) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := s.db.Exec(query, e.Timestamp, e.CorrelationID, e.NodeID, e.Action, e.ServiceName, e.Image, e.Status, e.Actor)
	if err != nil {
		log.Printf("[WARN] Failed to write orchestration event to DuckDB: %v", err)
	}
}

func (s *Server) writeAnomaly(a AnomalyEvent) {
	query := `INSERT INTO anomaly_history (timestamp, anomaly_id, node_id, type, description, severity, resolved) VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err := s.db.Exec(query, a.Timestamp, a.AnomalyID, a.NodeID, a.Type, a.Description, a.Severity, a.Resolved)
	if err != nil {
		log.Printf("[WARN] Failed to write anomaly to DuckDB: %v", err)
	}
}

// ─── HTTP API Handlers ───────────────────────────────────────────────────────

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	respondJSON(w, http.StatusOK, map[string]interface{}{
		"status":  "ok",
		"service": "cbal",
		"time":    time.Now().UTC(),
	})
}

// POST /query - Execute custom SQL OLAP queries on DuckDB
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query string `json:"query"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}

	// Safety check: enforce read-only commands for API execution
	cleanQuery := strings.TrimSpace(strings.ToLower(req.Query))
	if !strings.HasPrefix(cleanQuery, "select") && !strings.HasPrefix(cleanQuery, "show") && !strings.HasPrefix(cleanQuery, "describe") {
		respondJSON(w, http.StatusForbidden, map[string]string{"error": "only SELECT, SHOW, or DESCRIBE statements are allowed"})
		return
	}

	// Execute on DuckDB
	rows, err := s.db.QueryContext(r.Context(), req.Query)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	result := []map[string]interface{}{}
	for rows.Next() {
		columns := make([]interface{}, len(cols))
		columnPointers := make([]interface{}, len(cols))
		for i := range columns {
			columnPointers[i] = &columns[i]
		}

		if err := rows.Scan(columnPointers...); err != nil {
			respondJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
			return
		}

		rowMap := make(map[string]interface{})
		for i, colName := range cols {
			val := columns[i]
			rowMap[colName] = val
		}
		result = append(result, rowMap)
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"columns": cols,
		"rows":    result,
		"count":   len(result),
	})
}

// GET /insights - Retrieve real-time anomalies and risk scores
func (s *Server) handleInsights(w http.ResponseWriter, r *http.Request) {
	// Query unresolved anomalies
	rows, err := s.db.QueryContext(r.Context(), `SELECT timestamp, anomaly_id, node_id, type, description, severity FROM anomaly_history WHERE resolved = false ORDER BY timestamp DESC LIMIT 20`)
	anomalies := []AnomalyEvent{}
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var a AnomalyEvent
			if err := rows.Scan(&a.Timestamp, &a.AnomalyID, &a.NodeID, &a.Type, &a.Description, &a.Severity); err == nil {
				anomalies = append(anomalies, a)
			}
		}
	}

	// Calculate simple topology risk scores
	// Score calculation based on active anomalies and restart loops
	riskRows, err := s.db.QueryContext(r.Context(), `SELECT node_id, COUNT(*) as count FROM anomaly_history WHERE timestamp > CAST(now() AS TIMESTAMP) - INTERVAL '15 minutes' GROUP BY node_id`)
	riskScores := make(map[string]float64)
	if err == nil {
		defer riskRows.Close()
		for riskRows.Next() {
			var nodeID string
			var count int
			if err := riskRows.Scan(&nodeID, &count); err == nil {
				score := float64(count) * 0.2
				if score > 1.0 {
					score = 1.0
				}
				riskScores[nodeID] = score
			}
		}
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"unresolved_anomalies": anomalies,
		"topology_risk_scores": riskScores,
		"calculated_at":        time.Now().UTC(),
	})
}

// POST /compress - Memory Compression Lifecycle
func (s *Server) handleCompress(w http.ResponseWriter, r *http.Request) {
	// 1. Fetch raw metrics/logs from the past 24 hours
	rows, err := s.db.QueryContext(r.Context(), `SELECT node_id, metric_name, avg(value) as val FROM metric_history WHERE timestamp > CAST(now() AS TIMESTAMP) - INTERVAL '1 day' GROUP BY node_id, metric_name`)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to query compression targets: " + err.Error()})
		return
	}
	defer rows.Close()

	type summaryItem struct {
		Node   string  `json:"node"`
		Metric string  `json:"metric"`
		AvgVal float64 `json:"average"`
	}
	items := []summaryItem{}
	for rows.Next() {
		var item summaryItem
		if err := rows.Scan(&item.Node, &item.Metric, &item.AvgVal); err == nil {
			items = append(items, item)
		}
	}

	if len(items) == 0 {
		respondJSON(w, http.StatusOK, map[string]string{"message": "no data to compress"})
		return
	}

	// 2. Call AICP / NVIDIA LLM to synthesize the data
	log.Printf("[CBAL] Triggering memory compression for %d telemetry channels...", len(items))
	rawJSON, _ := json.Marshal(items)

	prompt := fmt.Sprintf(`Summarize the following 24-hour edge node metrics into a 1-paragraph dense semantic operational insight.
Metrics: %s`, string(rawJSON))

	aicpReqBody, _ := json.Marshal(map[string]interface{}{
		"prompt": prompt,
	})

	// Get auth token from requester context
	authHeader := r.Header.Get("Authorization")

	req, err := http.NewRequestWithContext(r.Context(), "POST", s.cfg.AicpURL+"/completions", bytes.NewReader(aicpReqBody))
	var summaryText string = "System operation: normal parameters."
	if err == nil {
		req.Header.Set("Content-Type", "application/json")
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}
		resp, err := s.httpClient.Do(req)
		if err == nil {
			defer resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				var aicpResp struct {
					Text string `json:"text"`
				}
				if err := json.NewDecoder(resp.Body).Decode(&aicpResp); err == nil {
					summaryText = aicpResp.Text
				}
			}
		} else {
			log.Printf("[WARN] Failed to connect to AICP for summarization: %v", err)
		}
	}

	// 3. Write summary back to SSI (Qdrant)
	ssiPayload, _ := json.Marshal(map[string]interface{}{
		"type":    "infrastructure_summary",
		"source":  "cbal",
		"content": summaryText,
		"tags":    []string{"cbal", "compression", "daily_summary"},
	})
	ssiURL := strings.Replace(s.cfg.AicpURL, "aicp:8004", "ssi:8003", 1) // Locate SSI URL
	ssiReq, err := http.NewRequest("POST", ssiURL+"/memory", bytes.NewReader(ssiPayload))
	if err == nil {
		ssiReq.Header.Set("Content-Type", "application/json")
		if authHeader != "" {
			ssiReq.Header.Set("Authorization", authHeader)
		}
		ssiResp, err := s.httpClient.Do(ssiReq)
		if err == nil {
			ssiResp.Body.Close()
		}
	}

	// 4. Prune the raw records from DuckDB to save disk/memory
	_, _ = s.db.Exec(`DELETE FROM metric_history WHERE timestamp < CAST(now() AS TIMESTAMP) - INTERVAL '1 day'`)
	_, _ = s.db.Exec(`DELETE FROM orchestration_history WHERE timestamp < CAST(now() AS TIMESTAMP) - INTERVAL '1 day'`)
	_, _ = s.db.Exec(`DELETE FROM anomaly_history WHERE timestamp < CAST(now() AS TIMESTAMP) - INTERVAL '1 day'`)

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"compressed":      true,
		"records_pruned":  "telemetry > 24h",
		"semantic_memory": summaryText,
	})
}

// ─── JWT Authentication Middleware ──────────────────────────────────────────

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

		next.ServeHTTP(w, r)
	})
}

func (s *Server) validateTokenRemote(ctx context.Context, token string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.cfg.SilURL+"/validate", nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := s.httpClient.Do(req)
	if err != nil {
		log.Printf("[JWT] SIL unavailable — failing open in development: %v", err)
		return nil
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("SIL returned status %d", resp.StatusCode)
	}

	var silResp struct {
		Valid bool   `json:"valid"`
		Error string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&silResp); err != nil {
		return fmt.Errorf("decode error: %w", err)
	}

	if !silResp.Valid {
		return fmt.Errorf("token invalid: %s", silResp.Error)
	}

	return nil
}

func respondJSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *Server) router() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/health", s.handleHealth)

	r.Group(func(r chi.Router) {
		r.Use(s.jwtMiddleware)
		r.Post("/query", s.handleQuery)
		r.Get("/insights", s.handleInsights)
		r.Post("/compress", s.handleCompress)
		r.Get("/datasets", s.handleListDatasets)
		r.Post("/datasets/import", s.handleImportDataset)
		r.Delete("/datasets/{name}", s.handleDeleteDataset)
	})

	return r
}

type ColumnInfo struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	NotNull    bool   `json:"notnull"`
	DefaultVal string `json:"dflt_value"`
	PK         bool   `json:"pk"`
}

type DatasetInfo struct {
	TableName string       `json:"table_name"`
	RowCount  int64        `json:"row_count"`
	Columns   []ColumnInfo `json:"columns"`
}

func isValidTableName(name string) bool {
	if len(name) == 0 {
		return false
	}
	for i, c := range name {
		if i == 0 {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_') {
				return false
			}
		} else {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_') {
				return false
			}
		}
	}
	return true
}

func (s *Server) handleListDatasets(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.QueryContext(r.Context(), "SHOW TABLES;")
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to list tables: " + err.Error()})
		return
	}
	defer rows.Close()

	tables := []string{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err == nil {
			tables = append(tables, name)
		}
	}

	datasets := []DatasetInfo{}
	for _, t := range tables {
		if !isValidTableName(t) {
			continue
		}

		var rowCount int64
		countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s", t)
		_ = s.db.QueryRowContext(r.Context(), countQuery).Scan(&rowCount)

		infoQuery := fmt.Sprintf("PRAGMA table_info('%s')", t)
		infoRows, err := s.db.QueryContext(r.Context(), infoQuery)
		columns := []ColumnInfo{}
		if err == nil {
			for infoRows.Next() {
				var cid int
				var name string
				var colType string
				var notnull bool
				var dfltVal sql.NullString
				var pk bool
				if err := infoRows.Scan(&cid, &name, &colType, &notnull, &dfltVal, &pk); err == nil {
					columns = append(columns, ColumnInfo{
						Name:       name,
						Type:       colType,
						NotNull:    notnull,
						DefaultVal: dfltVal.String,
						PK:         pk,
					})
				}
			}
			infoRows.Close()
		} else {
			log.Printf("[WARN] Failed to get table info for %s: %v", t, err)
		}

		datasets = append(datasets, DatasetInfo{
			TableName: t,
			RowCount:  rowCount,
			Columns:   columns,
		})
	}

	respondJSON(w, http.StatusOK, datasets)
}

func (s *Server) handleImportDataset(w http.ResponseWriter, r *http.Request) {
	var req struct {
		TableName  string `json:"table_name"`
		SourceType string `json:"source_type"`
		URL        string `json:"url"`
		CSVData    string `json:"csv_data"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid payload"})
		return
	}

	req.TableName = strings.TrimSpace(req.TableName)
	if !isValidTableName(req.TableName) {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "table name must start with a letter or underscore, and contain only letters, numbers, and underscores"})
		return
	}

	var count int
	checkQuery := fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = '%s'", req.TableName)
	err := s.db.QueryRowContext(r.Context(), checkQuery).Scan(&count)
	if err == nil && count > 0 {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("table '%s' already exists", req.TableName)})
		return
	}

	if req.SourceType == "url" {
		req.URL = strings.TrimSpace(req.URL)
		if req.URL == "" {
			respondJSON(w, http.StatusBadRequest, map[string]string{"error": "URL is required for URL source"})
			return
		}

		readFunc := "read_csv_auto"
		if strings.HasSuffix(strings.ToLower(req.URL), ".parquet") {
			readFunc = "read_parquet"
		} else if strings.HasSuffix(strings.ToLower(req.URL), ".json") {
			readFunc = "read_json_auto"
		}

		importQuery := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s('%s');", req.TableName, readFunc, req.URL)
		_, err := s.db.ExecContext(r.Context(), importQuery)
		if err != nil {
			respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to import dataset: " + err.Error()})
			return
		}
	} else if req.SourceType == "paste" {
		if strings.TrimSpace(req.CSVData) == "" {
			respondJSON(w, http.StatusBadRequest, map[string]string{"error": "CSV data is required for paste"})
			return
		}

		tempFilePath := fmt.Sprintf("/data/temp_import_%d.csv", time.Now().UnixNano())
		err := os.WriteFile(tempFilePath, []byte(req.CSVData), 0644)
		if err != nil {
			respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to write temporary file: " + err.Error()})
			return
		}
		defer os.Remove(tempFilePath)

		importQuery := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s');", req.TableName, tempFilePath)
		_, err = s.db.ExecContext(r.Context(), importQuery)
		if err != nil {
			respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to import dataset: " + err.Error()})
			return
		}
	} else {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid source_type (must be 'url' or 'paste')"})
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success":    true,
		"table_name": req.TableName,
		"message":    fmt.Sprintf("dataset successfully imported into table '%s'", req.TableName),
	})
}

func (s *Server) handleDeleteDataset(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "name")
	name = strings.TrimSpace(name)

	if !isValidTableName(name) {
		respondJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid table name"})
		return
	}

	protectedTables := map[string]bool{
		"metric_history":        true,
		"orchestration_history": true,
		"anomaly_history":       true,
	}
	if protectedTables[strings.ToLower(name)] {
		respondJSON(w, http.StatusForbidden, map[string]string{"error": "system analytics history tables cannot be deleted"})
		return
	}

	dropQuery := fmt.Sprintf("DROP TABLE %s;", name)
	_, err := s.db.ExecContext(r.Context(), dropQuery)
	if err != nil {
		respondJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to delete dataset: " + err.Error()})
		return
	}

	respondJSON(w, http.StatusOK, map[string]interface{}{
		"success":    true,
		"table_name": name,
		"message":    fmt.Sprintf("table '%s' successfully dropped", name),
	})
}

func main() {
	cfg := loadConfig()
	log.Printf("[CBAL] Starting CBAL API on port %s...", cfg.Port)

	srv, err := newServer(cfg)
	if err != nil {
		log.Fatalf("Fatal init server error: %v", err)
	}

	go srv.initNATS()
	go srv.initRedpanda()

	httpSrv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      srv.router(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Minute,
		IdleTimeout:  60 * time.Second,
	}

	if err := httpSrv.ListenAndServe(); err != nil {
		log.Fatalf("HTTP server error: %v", err)
	}
}
